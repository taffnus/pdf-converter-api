import os
import io
import zipfile
import tempfile
from typing import Optional, List
 
import psycopg2
from fastapi import FastAPI, File, UploadFile, Header, HTTPException
from fastapi.responses import Response
import pypdfium2 as pdfium
 
app = FastAPI()
 
# --- Konfiguration über Umgebungsvariablen (in Railway einträgst du diese unter "Variables") ---
DATABASE_URL = os.environ["DATABASE_URL"]  # aus Supabase: Project Settings -> Database -> Connection string
 
MAX_FILE_SIZE_MB = 10
MAX_PAGES = 5
MAX_PAGE_DIMENSION_PT = 3000
 
 
class PdfTooLargeError(Exception):
    pass
 
 
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)
 
 
def check_api_key(api_key: str) -> str:
    """Prüft den Key gegen die Supabase-Tabelle 'api_keys'. Gibt den Plan zurück oder wirft 401/403."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT plan, is_active FROM api_keys WHERE key = %s", (api_key,))
            row = cur.fetchone()
    finally:
        conn.close()
 
    if row is None:
        raise HTTPException(status_code=401, detail="Ungültiger API-Key")
    plan, is_active = row
    if not is_active:
        raise HTTPException(status_code=403, detail="API-Key ist deaktiviert")
    return plan
 
 
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
    check_api_key(x_api_key)  # wirft 401/403 bei ungültigem oder deaktiviertem Key
 
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
 
    return {"status": "ok"}
