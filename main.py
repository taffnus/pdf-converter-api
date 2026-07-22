import os
import io
import zipfile
import tempfile
from typing import Optional, List

import psycopg2
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
FRONTEND_URL = os.environ["FRONTEND_URL"]  # z.B. https://pdf-converter-website.edeka130208.workers.dev
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


class PdfTooLargeError(Exception):
    pass


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def get_profile(api_key: str) -> dict:
    """Holt das Profil zum API-Key aus der Supabase-Tabelle 'profiles'. Wirft 401 bei unbekanntem Key."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, plan, credits_remaining, monthly_usage FROM profiles WHERE api_key = %s",
                (api_key,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

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
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
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
        conn.commit()
    finally:
        conn.close()

 
def check_pdf_limits(pdf_path: str):
    size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
    if size_mb > MAX_FILE_SIZE_MB:
        raise PdfTooLargeError(f"Datei ist {size_mb:.1f} MB, erlaubt sind {MAX_FILE_SIZE_MB} MB")
 
    pdf = pdfium.PdfDocument(pdf_path)
    try:
        page_count = len(pdf)
        if page_count > MAX_PAGES:
            raise PdfTooLargeError(f"PDF hat {page_count} Seiten, erlaubt sind {MAX_PAGES}")
        for i in range(page_count):
            w, h = pdf[i].get_size()
            if w > MAX_PAGE_DIMENSION_PT or h > MAX_PAGE_DIMENSION_PT:
                raise PdfTooLargeError(f"Seite {i+1} ist zu groß ({w:.0f}x{h:.0f}pt)")
    finally:
        pdf.close()
 
 
def render_pdf_to_png_bytes(pdf_path: str, dpi: int = 200) -> List[bytes]:
    pdf = pdfium.PdfDocument(pdf_path)
    images = []
    try:
        for i in range(len(pdf)):
            page = pdf[i]
            bitmap = page.render(scale=dpi / 72)
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
    check_quota(profile)  # wirft 402 (Free, keine Credits) oder 429 (Pro, Fair-Use erreicht)

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        check_pdf_limits(tmp_path)
        images = render_pdf_to_png_bytes(tmp_path)
    except PdfTooLargeError as e:
        raise HTTPException(status_code=413, detail=str(e))
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

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT stripe_customer_id FROM profiles WHERE id = %s", (user_id,))
            row = cur.fetchone()
    finally:
        conn.close()

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
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE profiles SET plan = 'pro', stripe_customer_id = %s, monthly_usage = 0 "
                        "WHERE id = %s",
                        (customer_id, user_id),
                    )
                conn.commit()
            finally:
                conn.close()

    elif event["type"] in ("customer.subscription.deleted", "customer.subscription.updated"):
        subscription = event["data"]["object"]
        customer_id = subscription.get("customer")
        status = subscription.get("status")
        if status in ("canceled", "unpaid", "incomplete_expired"):
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE profiles SET plan = 'free' WHERE stripe_customer_id = %s",
                        (customer_id,),
                    )
                conn.commit()
            finally:
                conn.close()

    elif event["type"] == "invoice.paid":
        # Wird bei jeder erfolgreichen Abo-Zahlung ausgeloest (auch bei der ersten) --
        # setzt die Fair-Use-Zaehlung fuer den neuen Abrechnungszeitraum zurueck.
        invoice = event["data"]["object"]
        customer_id = invoice.get("customer")
        if customer_id:
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE profiles SET monthly_usage = 0 WHERE stripe_customer_id = %s",
                        (customer_id,),
                    )
                conn.commit()
            finally:
                conn.close()

    return {"received": True}
