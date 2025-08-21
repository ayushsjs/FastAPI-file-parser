"""
FastAPI File Parser CRUD API with Progress Tracking
==================================================

One-file implementation you can drop into a repo as `main.py`.
Includes:
- File upload with chunked saving + simulated progress
- Async parsing (CSV, Excel, PDF) in a background task
- Progress polling API and optional Server‑Sent Events endpoint
- CRUD (list, get content, delete)
- Simple API-key auth (optional via `API_KEY` env var)
- SQLite via SQLAlchemy (no migrations required)
- Minimal pytest examples in the bottom comment block

Run locally
-----------
1) Create venv and install deps:
   pip install "fastapi[standard]" sqlalchemy pandas openpyxl pdfplumber sse-starlette python-multipart uvicorn

2) Start API:
   uvicorn main:app --reload

3) (Optional) Set an API key (recommended):
   On Linux/macOS:  export API_KEY=dev123
   On Windows PS:   setx API_KEY dev123  (then restart your shell)

4) Test upload (CSV example):
   curl -X POST http://localhost:8000/files \
        -H "X-API-Key: dev123" \
        -F "file=@your.csv"

5) Poll progress:
   curl http://localhost:8000/files/{file_id}/progress -H "X-API-Key: dev123"

6) Get parsed content when ready:
   curl http://localhost:8000/files/{file_id} -H "X-API-Key: dev123"

Database file: `./data/app.db`; Uploaded files stored under `./data/uploads/`.

Notes
-----
- Progress is stored in-process (in-memory). In production, prefer Redis.
- SSE endpoint requires a client that keeps the HTTP connection open.
- Accepted types: CSV (.csv), Excel (.xls/.xlsx), and PDF (.pdf).
- Large-file handling: uploads are streamed to disk in 1MB chunks without loading the whole file into memory.
"""

from __future__ import annotations

import os
import io
import json
import uuid
import time
import shutil
import asyncio
import datetime as dt
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    File,
    HTTPException,
    Path,
    Query,
    UploadFile,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from pydantic import BaseModel

from sqlalchemy import (
    create_engine,
    String,
    Text,
    DateTime,
    Integer,
)
from sqlalchemy.orm import declarative_base, sessionmaker, Mapped, mapped_column

from fastapi import FastAPI

# Create the app


# Root endpoint


# Optional SSE support
try:
    from sse_starlette.sse import EventSourceResponse
    SSE_AVAILABLE = True
except Exception:
    SSE_AVAILABLE = False

# -------------------------
# Config
# -------------------------
DATA_DIR = os.path.join(os.getcwd(), "data")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
DB_URL = f"sqlite:///{os.path.join(DATA_DIR, 'app.db')}"
CHUNK_SIZE = 1024 * 1024  # 1 MB

os.makedirs(UPLOAD_DIR, exist_ok=True)

engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

# -------------------------
# Models
# -------------------------
class FileStatus:
    UPLOADING = "uploading"
    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"


class FileRecord(Base):
    __tablename__ = "files"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # uuid str
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    content_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default=FileStatus.UPLOADING)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    parsed_content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON string
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow, onupdate=dt.datetime.utcnow)


Base.metadata.create_all(engine)

# -------------------------
# In-memory progress store (replace with Redis in prod)
# -------------------------
progress_store: Dict[str, Dict[str, Any]] = {}

# -------------------------
# Schemas
# -------------------------
class FileMeta(BaseModel):
    id: str
    filename: str
    status: str
    size_bytes: int
    created_at: dt.datetime

class ProgressResp(BaseModel):
    file_id: str
    status: str
    progress: int

# -------------------------
# Auth dependency (simple API key)
# -------------------------
API_KEY = os.getenv("API_KEY")  # if None, auth is disabled

def require_api_key(x_api_key: Optional[str] = Query(default=None, alias="X-API-Key")):
    if API_KEY is None:
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

# -------------------------
# App lifecycle
# -------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # On startup, clear progress of any non-ready files to a sane state
    with SessionLocal() as db:
        for rec in db.query(FileRecord).all():
            if rec.status in {FileStatus.UPLOADING, FileStatus.PROCESSING}:
                progress_store[rec.id] = {"status": rec.status, "progress": 0}
    yield

app = FastAPI(
    title="File Parser CRUD API with Progress",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
def read_root():
    return {"message": "Welcome to File Parser API 🚀"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# -------------------------
# Helpers
# -------------------------

def _ext(filename: str) -> str:
    return os.path.splitext(filename)[1].lower()


def _is_supported(filename: str) -> bool:
    return _ext(filename) in {".csv", ".xls", ".xlsx", ".pdf"}


def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow()


async def _save_upload_stream(file: UploadFile, dest_path: str, file_id: str) -> int:
    """Stream the uploaded file to disk, updating progress based on bytes read.
    Returns total bytes written.
    """
    total_written = 0
    # If content-length is known, use it for percentage calculation; else simulate.
    content_length = 0
    try:
        # Many clients send Content-Length at the overall request level; UploadFile doesn't expose it directly.
        # We'll attempt to infer from SpooledTemporaryFile if available (best-effort). Otherwise, progress will be time-based.
        if hasattr(file.file, 'seek') and hasattr(file.file, 'tell'):
            cur = file.file.tell()
            file.file.seek(0, os.SEEK_END)
            content_length = file.file.tell()
            file.file.seek(cur)
    except Exception:
        content_length = 0

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "wb") as out:
        while True:
            chunk = await file.read(CHUNK_SIZE)
            if not chunk:
                break
            out.write(chunk)
            total_written += len(chunk)
            # Update progress approximately
            if content_length > 0:
                pct = int(min(99, (total_written / content_length) * 100))  # cap at 99% until processing
            else:
                # If unknown size, increment slowly up to 70% during upload
                prev = progress_store.get(file_id, {}).get("progress", 0)
                pct = min(70, prev + 5)
            progress_store[file_id] = {"status": FileStatus.UPLOADING, "progress": pct}
            await asyncio.sleep(0)  # yield to event loop
    return total_written


def _parse_csv(path: str) -> List[dict]:
    import pandas as pd
    df = pd.read_csv(path)
    return df.to_dict(orient="records")


def _parse_excel(path: str) -> Dict[str, List[dict]]:
    import pandas as pd
    xls = pd.ExcelFile(path)
    result: Dict[str, List[dict]] = {}
    for sheet in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet)
        result[sheet] = df.to_dict(orient="records")
    return result


def _parse_pdf(path: str) -> Dict[str, Any]:
    import pdfplumber
    pages: List[str] = []
    with pdfplumber.open(path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            pages.append(text)
    return {"pages": pages}


async def _parse_file_async(file_id: str):
    """Background task: parse file, store JSON in DB, update status/progress."""
    with SessionLocal() as db:
        rec = db.query(FileRecord).get(file_id)
        if not rec:
            progress_store[file_id] = {"status": FileStatus.FAILED, "progress": 0}
            return
        try:
            # Begin processing phase
            rec.status = FileStatus.PROCESSING
            db.commit()
            progress_store[file_id] = {"status": FileStatus.PROCESSING, "progress": max(progress_store.get(file_id, {}).get("progress", 0), 60)}

            ext = _ext(rec.filename)
            parsed: Any
            if ext == ".csv":
                parsed = _parse_csv(rec.content_path)
            elif ext in {".xls", ".xlsx"}:
                parsed = _parse_excel(rec.content_path)
            elif ext == ".pdf":
                parsed = _parse_pdf(rec.content_path)
            else:
                raise ValueError("Unsupported file type")

            # Simulate long processing for demo (optional)
            for step in range(60, 99, 5):
                progress_store[file_id] = {"status": FileStatus.PROCESSING, "progress": step}
                await asyncio.sleep(0.1)

            rec.parsed_content = json.dumps(parsed)
            rec.status = FileStatus.READY
            db.commit()
            progress_store[file_id] = {"status": FileStatus.READY, "progress": 100}
        except Exception as e:
            rec.status = FileStatus.FAILED
            db.commit()
            progress_store[file_id] = {"status": FileStatus.FAILED, "progress": progress_store.get(file_id, {}).get("progress", 0)}

# -------------------------
# Routes
# -------------------------
@app.post("/files", status_code=201)
async def upload_file(background: BackgroundTasks, file: UploadFile = File(...), auth: None = Depends(require_api_key)):
    if not _is_supported(file.filename):
        raise HTTPException(status_code=400, detail="Unsupported file type. Use CSV, Excel (.xls/.xlsx) or PDF.")

    file_id = str(uuid.uuid4())
    dest_path = os.path.join(UPLOAD_DIR, f"{file_id}_{os.path.basename(file.filename)}")

    # Pre-create DB record in UPLOADING state
    with SessionLocal() as db:
        rec = FileRecord(
            id=file_id,
            filename=file.filename,
            content_path=dest_path,
            status=FileStatus.UPLOADING,
            size_bytes=0,
            created_at=_now_utc(),
        )
        db.add(rec)
        db.commit()

    # Stream save
    progress_store[file_id] = {"status": FileStatus.UPLOADING, "progress": 0}
    try:
        size = await _save_upload_stream(file, dest_path, file_id)
    finally:
        await file.close()

    # Update DB size and kick off parsing
    with SessionLocal() as db:
        rec = db.query(FileRecord).get(file_id)
        if rec:
            rec.size_bytes = size
            db.commit()

    # Schedule background parsing
    background.add_task(_parse_file_async, file_id)

    return {"file_id": file_id, "status": FileStatus.UPLOADING}


@app.get("/files/{file_id}/progress", response_model=ProgressResp)
async def get_progress(file_id: str = Path(...), auth: None = Depends(require_api_key)):
    with SessionLocal() as db:
        rec = db.query(FileRecord).get(file_id)
        if not rec:
            raise HTTPException(status_code=404, detail="File not found")
    info = progress_store.get(file_id)
    if info is None:
        # If we restarted the server, synthesize progress from DB status
        default_progress = 100 if rec.status == FileStatus.READY else 0
        info = {"status": rec.status, "progress": default_progress}
        progress_store[file_id] = info
    return {"file_id": file_id, **info}


@app.get("/files/{file_id}")
async def get_file_content(file_id: str = Path(...), auth: None = Depends(require_api_key)):
    with SessionLocal() as db:
        rec = db.query(FileRecord).get(file_id)
        if not rec:
            raise HTTPException(status_code=404, detail="File not found")
        if rec.status != FileStatus.READY or not rec.parsed_content:
            return JSONResponse(
                status_code=202,
                content={"message": "File upload or processing in progress. Please try again later."},
            )
        try:
            content = json.loads(rec.parsed_content)
        except Exception:
            content = rec.parsed_content
        return {"file_id": rec.id, "filename": rec.filename, "content": content}


@app.get("/files", response_model=List[FileMeta])
async def list_files(limit: int = Query(100, ge=1, le=1000), auth: None = Depends(require_api_key)):
    with SessionLocal() as db:
        q = db.query(FileRecord).order_by(FileRecord.created_at.desc()).limit(limit)
        items = [
            FileMeta(
                id=r.id,
                filename=r.filename,
                status=r.status,
                size_bytes=r.size_bytes,
                created_at=r.created_at,
            )
            for r in q
        ]
    return items


@app.delete("/files/{file_id}", status_code=204)
async def delete_file(file_id: str = Path(...), auth: None = Depends(require_api_key)):
    with SessionLocal() as db:
        rec = db.query(FileRecord).get(file_id)
        if not rec:
            raise HTTPException(status_code=404, detail="File not found")
        # Delete file from disk
        try:
            if os.path.exists(rec.content_path):
                os.remove(rec.content_path)
        except Exception:
            pass
        # Remove DB record
        db.delete(rec)
        db.commit()
    # Clear progress
    progress_store.pop(file_id, None)
    return JSONResponse(status_code=204, content=None)


if SSE_AVAILABLE:
    @app.get("/files/{file_id}/events")
    async def progress_events(file_id: str, auth: None = Depends(require_api_key)):
        with SessionLocal() as db:
            if not db.query(FileRecord).get(file_id):
                raise HTTPException(status_code=404, detail="File not found")

        async def event_generator() -> AsyncGenerator[dict, None]:
            last_sent = None
            while True:
                info = progress_store.get(file_id)
                if info is None:
                    # If file is ready or failed and we missed, synthesize
                    with SessionLocal() as db:
                        rec = db.query(FileRecord).get(file_id)
                        if rec:
                            info = {"status": rec.status, "progress": 100 if rec.status == FileStatus.READY else 0}
                            progress_store[file_id] = info
                if info != last_sent and info is not None:
                    last_sent = info
                    yield {"event": "progress", "data": json.dumps({"file_id": file_id, **info})}
                    if info["status"] in {FileStatus.READY, FileStatus.FAILED}:
                        break
                await asyncio.sleep(0.5)

        return EventSourceResponse(event_generator())

# -------------------------
# -------- Tests ----------
# -------------------------
"""
# Save as tests/test_basic.py (example)

import io
import json
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_reject_unsupported_type():
    r = client.post("/files", files={"file": ("file.txt", b"hello", "text/plain")})
    assert r.status_code == 400


def test_upload_csv_and_flow(tmp_path):
    csv_bytes = b"a,b\n1,2\n3,4\n"
    r = client.post("/files", files={"file": ("data.csv", csv_bytes, "text/csv")})
    assert r.status_code == 201
    file_id = r.json()["file_id"]

    # Poll progress until ready
    for _ in range(50):
        pr = client.get(f"/files/{file_id}/progress")
        assert pr.status_code == 200
        status = pr.json()["status"]
        if status == "ready":
            break
    # Fetch content
    gc = client.get(f"/files/{file_id}")
    assert gc.status_code in (200, 202)

"""
