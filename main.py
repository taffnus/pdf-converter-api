import os
import io
import zipfile
import tempfile
from typing import Optional, List

import psycopg2
from fastapi import FastAPI, File, UploadFile, Header, HTTPException
from fastapi.responses import Response, JSONResponse
import pypdfium2 as pdfium

app = FastAPI()

DATABASE_URL = os.environ.environ.get("DATABASE_URL")

MAX_FILE_SIZE_MB = 10
MAX_PAGES = 20  # Für Paid-User erhöht, Limitierung läuft jetzt über Credits
MAX_PAGE_DIMENSION_PT = 4000

class PdfTooLargeError(Exception):
    pass

def get_db_connection():
    # Erlaubt stabileren Verbindungsaufbau zu Supabase
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def check_api_key_and_credits(api_key: str, required_credits: int = 1):
    """Prüft Key, Aktivitätsstatus und ob genügend Credits vorhanden sind."""
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL fehlt in den Railway-Variablen!")
        
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT plan, is_active, credits FROM api_keys WHERE key = %s", (api_key,))
            row = cur.fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Supabase Verbindungsfehler: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

    if row is None:
        raise HTTPException(status_code=401, detail="Ungültiger API-Key")
    
    plan, is_active, credits = row
    
    if not is_active:
        raise HTTPException(status_code=403, detail="API-Key ist deaktiviert")
        
    if credits < required_credits:
        raise HTTPException(
            status_code=403, 
            detail=f"Guthaben aufgebraucht. Benötigt: {required_credits}, Verfügbar: {credits}. Bitte aufrüsten!"
        )
        
    return plan

def deduct_credits(api_key: str, amount: int):
    """Zieht die verbrauchten Credits nach erfolgreicher Konvertierung ab."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE api_keys SET credits = credits - %s WHERE key = %s", (amount, api_key))
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
            raise PdfTooLargeError(f"PDF hat {page_count} Seiten, erlaubt sind maximal {MAX_PAGES} pro Upload")
        return page_count
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
    
    # Vorab-Check: Hat der User überhaupt noch mindestens 1 Credit?
    check_api_key_and_credits(x_api_key, required_credits=1)

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    try:
        # Seitenanzahl ermitteln
        page_count = check_pdf_limits(tmp_path)
        
        # Exakter Check: Reichen die Credits für alle Seiten dieses PDFs?
        check_api_key_and_credits(x_api_key, required_credits=page_count)
        
        # Rendern
        images = render_pdf_to_png_bytes(tmp_path)
        
        # Credits abziehen
        deduct_credits(x_api_key, page_count)
        
    except PdfTooLargeError as e:
        raise HTTPException(status_code=413, detail=str(e))
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Konvertierungsfehler: {str(e)}")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    if len(images) == 1:
        return Response(content=images[0], media_type="image/png")

    # Mehrseitige PDFs als ZIP
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        for i, img in enumerate(images):
            zf.writestr(f"page_{i+1}.png", img)
    return Response(content=zip_buf.getvalue(), media_type="application/zip")

@app.get("/health")
def health():
    return {"status": "ok"}
