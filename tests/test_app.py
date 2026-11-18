import io
import os
import time
import uuid
import zipfile

import pytest
from pypdf import PdfReader
from reportlab.pdfgen import canvas

import app as app_module


def make_pdf(pages: int, label: str, filler: int = 3000) -> bytes:
    """各ページの先頭行に「<label> page <番号>」と書いた PDF を作る。filler でページの大きさを調整する。"""
    buf = io.BytesIO()
    c = canvas.Canvas(buf)
    for i in range(pages):
        c.drawString(100, 700, f"{label} page {i + 1}")
        c.drawString(10, 650, os.urandom(filler).hex())
        c.showPage()
    c.save()
    return buf.getvalue()


@pytest.fixture
def client():
    return app_module.app.test_client()


def upload(client, sid: str, name: str, data: bytes, chunks: int = 1):
    size = -(-len(data) // chunks)
    for i in range(chunks):
        part = data[i * size:(i + 1) * size]
        r = client.post("/api/upload_chunk", content_type="multipart/form-data", data={
            "session_id": sid, "chunk_index": str(i), "total_chunks": str(chunks),
            "filename": name, "data": (io.BytesIO(part), name),
        })
        assert r.status_code == 200, r.get_json()


def wait_for_job(client, job_id: str) -> dict:
    deadline = time.time() + 60
    while time.time() < deadline:
        status = client.get(f"/api/status/{job_id}").get_json()
        if status["status"] != "running":
            return status
        time.sleep(0.2)
    pytest.fail("ジョブが時間内に終わりませんでした")


def page_labels(pdf: bytes) -> list[str]:
    return [page.extract_text().split("\n")[0] for page in PdfReader(io.BytesIO(pdf)).pages]


def test_merges_in_order_and_splits_under_limit(client):
    sid = str(uuid.uuid4())
    files = {"a.pdf": make_pdf(30, "A"), "b.pdf": make_pdf(20, "B")}
    for name, data in files.items():
        upload(client, sid, name, data, chunks=2)  # チャンクに分けて送っても元どおりに組み立てられる
    limit_mb = sum(len(d) for d in files.values()) / 1024 / 1024 / 3

    r = client.post("/api/finalize", data={
        "session_id": sid, "size_mb": str(limit_mb), "prefix": "out", "file_order": '["a.pdf", "b.pdf"]',
    })
    assert r.status_code == 200
    status = wait_for_job(client, r.get_json()["job_id"])
    assert status["status"] == "done"
    assert len(status["output_files"]) >= 3

    labels = []
    for name in status["output_files"]:
        pdf = client.get(f"/api/download/{sid}/{name}").data
        assert len(pdf) < limit_mb * 1024 * 1024
        labels += page_labels(pdf)
    # 指定した順に結合され、ページの抜けや重複がない
    assert labels == [f"A page {i}" for i in range(1, 31)] + [f"B page {i}" for i in range(1, 21)]

    archive = zipfile.ZipFile(io.BytesIO(client.get(f"/api/download_zip/{sid}").data))
    assert sorted(archive.namelist()) == sorted(status["output_files"])


def test_page_larger_than_limit_is_output_alone_with_warning(client):
    sid = str(uuid.uuid4())
    upload(client, sid, "a.pdf", make_pdf(3, "A", filler=5000))
    r = client.post("/api/finalize", data={"session_id": sid, "size_mb": "0.003"})
    status = wait_for_job(client, r.get_json()["job_id"])
    assert status["status"] == "done"
    assert len(status["output_files"]) == 3
    assert any("上限超え" in line for line in status["log"])


def test_broken_pdf_reports_error(client):
    sid = str(uuid.uuid4())
    upload(client, sid, "a.pdf", b"this is not a pdf")
    r = client.post("/api/finalize", data={"session_id": sid, "size_mb": "1"})
    assert wait_for_job(client, r.get_json()["job_id"])["status"] == "error"


@pytest.mark.parametrize("form", [
    {"session_id": "../../etc", "chunk_index": "0", "total_chunks": "1"},
    {"session_id": str(uuid.uuid4()), "chunk_index": "x", "total_chunks": "1"},
    {"session_id": str(uuid.uuid4()), "chunk_index": "1", "total_chunks": "1"},
])
def test_upload_rejects_invalid_input(client, form):
    data = {**form, "filename": "a.pdf", "data": (io.BytesIO(b"%PDF"), "a.pdf")}
    r = client.post("/api/upload_chunk", data=data, content_type="multipart/form-data")
    assert r.status_code == 400


@pytest.mark.parametrize("form", [
    {"size_mb": "0"},
    {"size_mb": "-1"},
    {"size_mb": "abc"},
    {"size_mb": "nan"},
    {"file_order": "{broken"},
    {"file_order": '{"a": 1}'},
])
def test_finalize_rejects_invalid_input(client, form):
    sid = str(uuid.uuid4())
    upload(client, sid, "a.pdf", make_pdf(1, "A"))
    r = client.post("/api/finalize", data={"session_id": sid, **form})
    assert r.status_code == 400


def test_download_cannot_escape_output_directory(client):
    sid = str(uuid.uuid4())
    upload(client, sid, "a.pdf", make_pdf(2, "A"))
    r = client.post("/api/finalize", data={"session_id": sid, "size_mb": "5"})
    wait_for_job(client, r.get_json()["job_id"])
    assert client.get(f"/api/download/{sid}/..%2F..%2Fuploads%2Fa.pdf").status_code == 404
    assert client.get(f"/api/download/{uuid.uuid4()}/out_001.pdf").status_code == 404
