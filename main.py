import os
import io
import time
import zipfile
import tempfile
import secrets
import threading
from collections import deque
from contextlib import contextmanager
from typing import Optional, List

import psycopg2
from psycopg2 import pool as pg_pool
import requests
import stripe
from fastapi import FastAPI, File, UploadFile, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
import pypdfium2 as pdfium

app = FastAPI()

# --- Konfiguration über Umgebungsvariablen (in Render einträgst du diese unter "Environment") ---
DATABASE_URL = os.environ["DATABASE_URL"]  # aus Supabase: Project Settings -> Database -> Connection string
SUPABASE_URL = os.environ["SUPABASE_URL"]  # z.B. https://xxxx.supabase.co
SUPABASE_ANON_KEY = os.environ["SUPABASE_ANON_KEY"]
FRONTEND_URL = os.environ["FRONTEND_URL"]  # z.B. https://pixelpdf.dev
STRIPE_SECRET_KEY = os.environ["STRIPE_SECRET_KEY"]
STRIPE_WEBHOOK_SECRET = os.environ["STRIPE_WEBHOOK_SECRET"]
STRIPE_PRICE_ID = os.environ["STRIPE_PRICE_ID"]

stripe.api_key = STRIPE_SECRET_KEY
stripe.api_version = "2025-03-31.basil"  # von Stripe gefordert (Managed Payments), aeltere Version schlaegt fehl

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL],
    allow_methods=["POST"],
    allow_headers=["Authorization", "Content-Type"],
)

MAX_FILE_SIZE_MB = 10
MAX_PAGES = 5
MAX_PAGE_DIMENSION_PT = 3000
FAIR_USE_LIMIT_PRO = 2000  # Konvertierungen/Monat für Pro-Plan

MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
UPLOAD_CHUNK_SIZE = 1024 * 1024  # in solchen Haeppchen wird der Upload von der Leitung gelesen
DB_POOL_MAX = 10  # reicht fuer einen uvicorn-Worker und bleibt weit unter dem Supabase-Limit
DB_CHECKOUT_ATTEMPTS = 3

# Obergrenze fuer die laengste Kante des gerenderten Bildes.
# MAX_PAGE_DIMENSION_PT begrenzt die Seite, nicht das Bild: 3000pt bei 200 DPI sind rund
# 8300 Pixel pro Kante, also ~280 MB allein fuer die Bitmap, plus eine zweite Kopie durch
# to_pil(). Eine einzige regelkonforme Seite konnte damit den Speicher des Servers sprengen.
# A4 hat bei 200 DPI etwa 1650x2340 Pixel, normale Dokumente werden also nie angefasst.
MAX_RENDER_PX_PER_SIDE = 4000

# Wie viele Konvertierungen ein einzelnes Konto pro Minute ausloesen darf.
# Das Rendern haengt am Prozessor und blockiert, und es laeuft nur ein uvicorn-Worker:
# ein einzelner Aufrufer, der /convert im Dauerfeuer benutzt, legt damit den Dienst fuer
# alle anderen lahm. Die Grenze deckelt ausserdem das Abgrasen der Gratis-Credits, wo eine
# weitere E-Mail-Adresse der einzige Preis fuer weitere 200 Credits ist.
RATE_LIMIT_PER_MINUTE = 30
RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_SWEEP_INTERVAL_SECONDS = 300  # wie oft leere Eintraege aufgeraeumt werden


class PdfTooLargeError(Exception):
    pass


class PdfUnreadableError(Exception):
    """Die Datei laesst sich nicht als PDF oeffnen oder rendern (kaputt, leer, kein PDF,
    passwortgeschuetzt). Fuehrt zu 400, nicht zu 500."""
    pass


# --- Ratenbegrenzung ---
# Gleitendes Zeitfenster im Arbeitsspeicher. Das reicht, solange genau ein uvicorn-Worker
# laeuft: der Zaehler lebt im selben Prozess wie das Rendern, das er schuetzen soll.
# Bei mehreren Workern oder mehreren Instanzen zaehlt jeder Prozess fuer sich, dann braucht
# es einen gemeinsamen Speicher (Redis) statt dieses Dictionaries.
_rate_limit_hits = {}
_rate_limit_lock = threading.Lock()
_rate_limit_last_sweep = 0.0


def check_rate_limit(user_id: str):
    """Wirft 429, wenn dieses Konto das Minutenlimit ausgeschoepft hat.

    Wichtig: gezaehlt wird VOR der Konvertierung, im Gegensatz zum Kontingent in
    consume_quota, das erst nach einem Erfolg zaehlt. Das ist kein Widerspruch. Ein Credit
    darf eine misslungene Konvertierung nicht kosten, Rechenzeit hat sie aber trotzdem
    gekostet, und genau die wird hier begrenzt. Wuerde erst nach dem Erfolg gezaehlt,
    koennte man den Server mit lauter fehlschlagenden Anfragen unbegrenzt beschaeftigen.

    Gezaehlt wird nach Profil-ID aus der Datenbank, nicht nach dem uebergebenen API-Key.
    Ein Angreifer koennte sonst mit jeder Anfrage einen neuen erfundenen Key schicken und
    dieses Dictionary unbegrenzt wachsen lassen, bis der Speicher voll ist.
    """
    global _rate_limit_last_sweep
    now = time.monotonic()
    cutoff = now - RATE_LIMIT_WINDOW_SECONDS

    with _rate_limit_lock:
        hits = _rate_limit_hits.setdefault(user_id, deque())
        while hits and hits[0] <= cutoff:
            hits.popleft()

        if len(hits) >= RATE_LIMIT_PER_MINUTE:
            retry_after = max(1, int(hits[0] + RATE_LIMIT_WINDOW_SECONDS - now) + 1)
            raise HTTPException(
                status_code=429,
                detail=(
                    f"Zu viele Anfragen. Erlaubt sind {RATE_LIMIT_PER_MINUTE} Konvertierungen "
                    f"pro Minute. Bitte in {retry_after} Sekunden erneut versuchen."
                ),
                headers={"Retry-After": str(retry_after)},
            )

        hits.append(now)

        # Konten, die laenger nichts mehr geschickt haben, hinterlassen sonst dauerhaft
        # eine leere Deque im Dictionary.
        if now - _rate_limit_last_sweep > RATE_LIMIT_SWEEP_INTERVAL_SECONDS:
            _rate_limit_last_sweep = now
            for stale in [k for k, v in _rate_limit_hits.items() if not v or v[-1] <= cutoff]:
                del _rate_limit_hits[stale]


# --- Datenbank-Verbindungen ---
# Frueher baute jede einzelne Anfrage eine eigene Verbindung auf und warf sie danach weg.
# Der Verbindungsaufbau dauert ein Vielfaches der eigentlichen Abfrage, und unter Last
# reisst das irgendwann das Verbindungslimit von Supabase. Der Pool haelt stattdessen eine
# Handvoll Verbindungen offen und reicht sie durch.
_db_pool = None
_db_pool_lock = threading.Lock()


def get_db_pool():
    """Legt den Pool beim ersten Zugriff an, nicht beim Import -- ein kurzer Datenbank-
    Aussetzer waehrend eines Deploys soll den Serverstart nicht verhindern."""
    global _db_pool
    if _db_pool is None:
        with _db_pool_lock:
            if _db_pool is None:
                _db_pool = pg_pool.ThreadedConnectionPool(1, DB_POOL_MAX, DATABASE_URL)
    return _db_pool


@contextmanager
def db_cursor(commit: bool = False):
    """Leiht eine Verbindung aus dem Pool und gibt sie danach zurueck.

    Tote Verbindungen werden vor der Ausgabe aussortiert: Supabase trennt inaktive
    Verbindungen nach einiger Zeit, und ohne diese Pruefung wuerde die erste Anfrage nach
    einer Ruhephase zuverlaessig fehlschlagen.
    """
    pool = get_db_pool()

    conn = None
    for _ in range(DB_CHECKOUT_ATTEMPTS):
        candidate = pool.getconn()
        try:
            with candidate.cursor() as cur:
                cur.execute("SELECT 1")
            candidate.rollback()
            conn = candidate
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            pool.putconn(candidate, close=True)

    if conn is None:
        raise HTTPException(status_code=503, detail="Datenbank gerade nicht erreichbar")

    healthy = True
    try:
        with conn.cursor() as cur:
            yield cur
        if commit:
            conn.commit()
        else:
            conn.rollback()  # auch Lesezugriffe sauber beenden, sonst bleibt die Verbindung "idle in transaction"
    except Exception:
        healthy = False
        try:
            conn.rollback()
        except psycopg2.Error:
            pass
        raise
    finally:
        pool.putconn(conn, close=not healthy)


def get_profile(api_key: str) -> dict:
    """Holt das Profil zum API-Key aus der Supabase-Tabelle 'profiles'. Wirft 401 bei unbekanntem Key."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT id, plan, credits_remaining, monthly_usage FROM profiles WHERE api_key = %s",
            (api_key,),
        )
        row = cur.fetchone()

    if row is None:
        raise HTTPException(status_code=401, detail="Ungültiger API-Key")

    profile_id, plan, credits_remaining, monthly_usage = row
    return {
        "id": profile_id,
        "plan": plan,
        "credits_remaining": credits_remaining,
        "monthly_usage": monthly_usage,
    }


def check_quota(profile: dict):
    """Prüft, ob noch Kontingent übrig ist, BEVOR konvertiert wird. Wirft 402/429, wenn nicht."""
    if profile["plan"] == "pro":
        if profile["monthly_usage"] >= FAIR_USE_LIMIT_PRO:
            raise HTTPException(
                status_code=429,
                detail=f"Fair-Use-Grenze von {FAIR_USE_LIMIT_PRO} Konvertierungen/Monat erreicht",
            )
    else:
        if profile["credits_remaining"] <= 0:
            raise HTTPException(
                status_code=402,
                detail="Keine Credits mehr übrig. Bitte auf Pro upgraden.",
            )


def consume_quota(profile: dict):
    """Zählt das Kontingent runter/hoch. Wird nur nach einer ERFOLGREICHEN Konvertierung aufgerufen."""
    with db_cursor(commit=True) as cur:
        if profile["plan"] == "pro":
            cur.execute(
                "UPDATE profiles SET monthly_usage = monthly_usage + 1 "
                "WHERE id = %s AND monthly_usage < %s",
                (profile["id"], FAIR_USE_LIMIT_PRO),
            )
        else:
            cur.execute(
                "UPDATE profiles SET credits_remaining = credits_remaining - 1 "
                "WHERE id = %s AND credits_remaining > 0",
                (profile["id"],),
            )

 
def open_pdf(pdf_path: str) -> pdfium.PdfDocument:
    """Oeffnet die Datei mit pdfium und uebersetzt jeden Ladefehler in PdfUnreadableError.

    Ohne das wird aus einer kaputten, leeren oder gar nicht als PDF gemeinten Datei ein
    unbehandelter Fehler und damit ein 500er: der Aufrufer bekaeme "Serverfehler" gemeldet,
    obwohl der Server einwandfrei arbeitet und schlicht die hochgeladene Datei defekt ist.
    """
    try:
        return pdfium.PdfDocument(pdf_path)
    except pdfium.PdfiumError as e:
        # pdfium unterscheidet die Ursachen nur im Meldungstext, eine eigene Fehlerklasse
        # pro Ursache gibt es nicht.
        if "password" in str(e).lower():
            raise PdfUnreadableError(
                "PDF ist passwortgeschützt. Bitte den Schutz vor dem Hochladen entfernen."
            )
        raise PdfUnreadableError(
            "Datei konnte nicht als PDF gelesen werden. Sie ist beschädigt, leer oder kein PDF."
        )


def check_pdf_limits(pdf_path: str):
    size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
    if size_mb > MAX_FILE_SIZE_MB:
        raise PdfTooLargeError(f"Datei ist {size_mb:.1f} MB, erlaubt sind {MAX_FILE_SIZE_MB} MB")

    pdf = open_pdf(pdf_path)
    try:
        page_count = len(pdf)
        if page_count > MAX_PAGES:
            raise PdfTooLargeError(f"PDF hat {page_count} Seiten, erlaubt sind {MAX_PAGES}")
        for i in range(page_count):
            try:
                w, h = pdf[i].get_size()
            except pdfium.PdfiumError:
                # Das Dokument liess sich oeffnen, diese eine Seite ist aber unbrauchbar.
                raise PdfUnreadableError(f"Seite {i+1} ist beschädigt und konnte nicht gelesen werden.")
            if w > MAX_PAGE_DIMENSION_PT or h > MAX_PAGE_DIMENSION_PT:
                raise PdfTooLargeError(f"Seite {i+1} ist zu groß ({w:.0f}x{h:.0f}pt)")
    finally:
        pdf.close()
 
 
async def save_upload_to_tempfile(file: UploadFile) -> str:
    """Schreibt den Upload stueckweise auf die Platte und bricht ab, sobald das Groessenlimit
    ueberschritten ist.

    Wichtig: die Datei erst komplett einzulesen (file.read() ohne Argument) und danach die
    Groesse zu pruefen, reicht aus, um den Server mit einem einzigen sehr grossen Upload
    ueber den Arbeitsspeicher umzubringen. Deshalb wird hier haeppchenweise gelesen und
    mitgezaehlt.
    """
    written = 0
    too_large = False

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp_path = tmp.name
        while True:
            chunk = await file.read(UPLOAD_CHUNK_SIZE)
            if not chunk:
                break
            written += len(chunk)
            if written > MAX_FILE_SIZE_BYTES:
                too_large = True
                break
            tmp.write(chunk)

    if too_large:
        os.remove(tmp_path)
        raise HTTPException(
            status_code=413,
            detail=f"Datei ist größer als {MAX_FILE_SIZE_MB} MB",
        )

    return tmp_path


def render_pdf_to_png_bytes(pdf_path: str, dpi: int = 200) -> List[bytes]:
    pdf = open_pdf(pdf_path)
    images = []
    try:
        for i in range(len(pdf)):
            try:
                page = pdf[i]
                scale = dpi / 72
                longest_side_pt = max(page.get_size())
                if longest_side_pt > 0 and longest_side_pt * scale > MAX_RENDER_PX_PER_SIDE:
                    scale = MAX_RENDER_PX_PER_SIDE / longest_side_pt  # grosse Seiten kleiner rendern statt Speicher fressen
                bitmap = page.render(scale=scale)
            except pdfium.PdfiumError:
                raise PdfUnreadableError(f"Seite {i+1} konnte nicht gerendert werden, die Datei ist beschädigt.")
            buf = io.BytesIO()
            bitmap.to_pil().save(buf, format="PNG")
            images.append(buf.getvalue())
            page.close()
    finally:
        pdf.close()
    return images
 
 
@app.post("/convert")
async def convert(file: UploadFile = File(...), x_api_key: Optional[str] = Header(None)):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Header 'X-API-Key' fehlt")

    profile = get_profile(x_api_key)  # wirft 401 bei unbekanntem Key
    check_rate_limit(profile["id"])  # wirft 429, bevor Rechenzeit verbraucht wird
    check_quota(profile)  # wirft 402 (Free, keine Credits) oder 429 (Pro, Fair-Use erreicht)

    tmp_path = await save_upload_to_tempfile(file)  # wirft 413, bevor grosse Dateien Speicher fressen

    try:
        check_pdf_limits(tmp_path)
        images = render_pdf_to_png_bytes(tmp_path)
    except PdfTooLargeError as e:
        raise HTTPException(status_code=413, detail=str(e))
    except PdfUnreadableError as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        os.remove(tmp_path)

    consume_quota(profile)  # erst NACH erfolgreicher Konvertierung zählen

    if len(images) == 1:
        return Response(content=images[0], media_type="image/png")
 
    # Mehrseitige PDFs: alle PNGs gezippt zurückgeben
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        for i, img in enumerate(images):
            zf.writestr(f"page_{i+1}.png", img)
    return Response(content=zip_buf.getvalue(), media_type="application/zip")
 
 
@app.get("/health")
def health():
    """Für das Uptime-Monitoring aus der Checkliste."""
    return {"status": "ok"}


def get_user_id_from_token(access_token: str) -> str:
    """Fragt Supabase, zu welchem eingeloggten Nutzer dieses Login-Token gehört."""
    resp = requests.get(
        f"{SUPABASE_URL}/auth/v1/user",
        headers={"Authorization": f"Bearer {access_token}", "apikey": SUPABASE_ANON_KEY},
        timeout=10,
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Ungültige oder abgelaufene Sitzung")
    return resp.json()["id"]


@app.post("/account/regenerate-api-key")
async def regenerate_api_key(authorization: Optional[str] = Header(None)):
    """Setzt einen neuen API-Key fuer den eingeloggten Nutzer; der alte wird sofort ungueltig.

    Laeuft bewusst ueber den Server und nicht direkt aus dem Browser: der Browser darf per
    RLS nicht in 'profiles' schreiben, sonst koennte sich dort jeder selbst Plan und Credits
    hochsetzen.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization-Header fehlt")

    user_id = get_user_id_from_token(authorization.removeprefix("Bearer "))
    # gleiches Format wie der Spaltenstandard in Supabase (gen_random_bytes(16) als Hex),
    # damit neu erzeugte Keys nicht anders aussehen als die bei der Registrierung
    new_key = secrets.token_hex(16)

    with db_cursor(commit=True) as cur:
        cur.execute(
            "UPDATE profiles SET api_key = %s WHERE id = %s RETURNING api_key",
            (new_key, user_id),
        )
        row = cur.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Profil nicht gefunden")

    return {"api_key": row[0]}


@app.post("/billing/create-checkout-session")
async def create_checkout_session(authorization: Optional[str] = Header(None)):
    """Wird vom 'Pro werden'-Button im Dashboard aufgerufen. Gibt eine Stripe-Checkout-URL zurück."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization-Header fehlt")

    user_id = get_user_id_from_token(authorization.removeprefix("Bearer "))

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
            client_reference_id=user_id,
            success_url=f"{FRONTEND_URL}/dashboard.html?upgraded=1",
            cancel_url=f"{FRONTEND_URL}/dashboard.html",
        )
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=f"Stripe error: {e.user_message or str(e)}")

    return {"url": session.url}


@app.post("/billing/create-portal-session")
async def create_portal_session(authorization: Optional[str] = Header(None)):
    """Wird vom 'Manage subscription'-Link im Dashboard aufgerufen. Fuehrt zum Stripe-
    Kundenportal, wo Nutzer selbst kuendigen, die Zahlungsmethode aendern oder Rechnungen einsehen koennen."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization-Header fehlt")

    user_id = get_user_id_from_token(authorization.removeprefix("Bearer "))

    with db_cursor() as cur:
        cur.execute("SELECT stripe_customer_id FROM profiles WHERE id = %s", (user_id,))
        row = cur.fetchone()

    if not row or not row[0]:
        raise HTTPException(status_code=404, detail="Kein aktives Pro-Abo gefunden")

    try:
        portal_session = stripe.billing_portal.Session.create(
            customer=row[0],
            return_url=f"{FRONTEND_URL}/dashboard.html",
        )
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=f"Stripe error: {e.user_message or str(e)}")

    return {"url": portal_session.url}


@app.post("/billing/webhook")
async def stripe_webhook(request: Request):
    """Empfängt Zahlungs-Events von Stripe und aktualisiert den Plan in Supabase."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except (ValueError, stripe.error.SignatureVerificationError):
        raise HTTPException(status_code=400, detail="Ungültige Webhook-Signatur")

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        user_id = session.get("client_reference_id")
        customer_id = session.get("customer")
        if user_id:
            with db_cursor(commit=True) as cur:
                cur.execute(
                    "UPDATE profiles SET plan = 'pro', stripe_customer_id = %s, monthly_usage = 0 "
                    "WHERE id = %s",
                    (customer_id, user_id),
                )

    elif event["type"] in ("customer.subscription.deleted", "customer.subscription.updated"):
        subscription = event["data"]["object"]
        customer_id = subscription.get("customer")
        status = subscription.get("status")
        if status in ("canceled", "unpaid", "incomplete_expired"):
            with db_cursor(commit=True) as cur:
                cur.execute(
                    "UPDATE profiles SET plan = 'free' WHERE stripe_customer_id = %s",
                    (customer_id,),
                )

    elif event["type"] == "invoice.paid":
        # Wird bei jeder erfolgreichen Abo-Zahlung ausgeloest (auch bei der ersten) --
        # setzt die Fair-Use-Zaehlung fuer den neuen Abrechnungszeitraum zurueck.
        invoice = event["data"]["object"]
        customer_id = invoice.get("customer")
        if customer_id:
            with db_cursor(commit=True) as cur:
                cur.execute(
                    "UPDATE profiles SET monthly_usage = 0 WHERE stripe_customer_id = %s",
                    (customer_id,),
                )

    return {"received": True}
