import os
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
import zipfile
from pathlib import Path

from flask import Flask, jsonify, render_template, request, send_file, session

app = Flask(__name__)
app.secret_key = os.urandom(24)

# ジョブ管理（メモリ内）
jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()

UPLOAD_FOLDER = Path(tempfile.gettempdir()) / "pdf-splitter_uploads"
OUTPUT_FOLDER = Path(tempfile.gettempdir()) / "pdf-splitter_outputs"
UPLOAD_FOLDER.mkdir(exist_ok=True)
OUTPUT_FOLDER.mkdir(exist_ok=True)


# ─── qpdf ヘルパー ───────────────────────────────────────────

def get_page_count(pdf_path: str) -> int:
    r = subprocess.run(["qpdf", "--show-npages", pdf_path],
                       capture_output=True, text=True, check=True)
    return int(r.stdout.strip())


def merge_pdfs(input_paths: list[str], output_path: str):
    if len(input_paths) == 1:
        shutil.copy2(input_paths[0], output_path)
        return
    cmd = ["qpdf", "--empty", "--pages"] + input_paths + ["--", output_path]
    subprocess.run(cmd, check=True, capture_output=True)


def extract_pages(src: str, dst: str, start: int, end: int):
    """1-indexed inclusive"""
    subprocess.run(
        ["qpdf", src, "--pages", ".", f"{start}-{end}", "--", dst],
        check=True, capture_output=True
    )


# ─── 分割ロジック ────────────────────────────────────────────

def split_worker(job_id: str, input_paths: list[str], limit_bytes: int,
                 prefix: str, output_dir: Path):
    def log(msg: str):
        with jobs_lock:
            jobs[job_id]["log"].append(msg)

    def set_progress(pct: int):
        with jobs_lock:
            jobs[job_id]["progress"] = pct

    def set_status(s: str):
        with jobs_lock:
            jobs[job_id]["status"] = s

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # Step 1: 結合
            log("📂 PDFを読み込んで結合中...")
            set_progress(5)
            merged = os.path.join(tmpdir, "merged.pdf")
            merge_pdfs(input_paths, merged)
            total_pages = get_page_count(merged)
            merged_size = os.path.getsize(merged)
            log(f"✅ 結合完了: {merged_size/1024/1024:.1f} MB, {total_pages} ページ")
            set_progress(15)

            # Step 2: 二分探索でチャンク計画
            log(f"🔍 分割計画を立てています（上限: {limit_bytes/1024/1024:.0f} MB）...")
            bytes_per_page = merged_size / max(total_pages, 1)

            chunks: list[tuple[int, int]] = []
            page_ptr = 1

            while page_ptr <= total_pages:
                # 推定ページ数（多めに見積もる）
                est_pages = max(1, int(limit_bytes / bytes_per_page * 0.95))
                hi = min(page_ptr + est_pages - 1, total_pages)
                lo = page_ptr
                best_end = page_ptr

                iteration = 0
                while lo <= hi:
                    mid = (lo + hi) // 2
                    test_out = os.path.join(tmpdir, f"t_{page_ptr}_{mid}.pdf")
                    extract_pages(merged, test_out, page_ptr, mid)
                    sz = os.path.getsize(test_out)
                    os.remove(test_out)
                    log(f"  試行: p{page_ptr}–p{mid} → {sz/1024/1024:.1f} MB {'✓' if sz <= limit_bytes else '✗'}")

                    if sz <= limit_bytes:
                        best_end = mid
                        lo = mid + 1
                    else:
                        hi = mid - 1

                    iteration += 1
                    prog = 15 + int((page_ptr / total_pages) * 60)
                    set_progress(min(prog, 74))

                # ページ範囲が最後まで届かない場合、残りも詰め込めるか確認
                if best_end < total_pages and best_end == min(page_ptr + est_pages - 1, total_pages):
                    # 推定上限まで達したが、まだ余裕があるかもしれないので伸ばす
                    extend_hi = total_pages
                    extend_lo = best_end + 1
                    while extend_lo <= extend_hi:
                        mid2 = (extend_lo + extend_hi) // 2
                        test_out = os.path.join(tmpdir, f"ext_{page_ptr}_{mid2}.pdf")
                        extract_pages(merged, test_out, page_ptr, mid2)
                        sz = os.path.getsize(test_out)
                        os.remove(test_out)
                        log(f"  拡張試行: p{page_ptr}–p{mid2} → {sz/1024/1024:.1f} MB {'✓' if sz <= limit_bytes else '✗'}")
                        if sz <= limit_bytes:
                            best_end = mid2
                            extend_lo = mid2 + 1
                        else:
                            extend_hi = mid2 - 1

                chunks.append((page_ptr, best_end))
                log(f"✅ チャンク {len(chunks)}: p{page_ptr}–p{best_end} ({best_end - page_ptr + 1} ページ)")
                page_ptr = best_end + 1

            log(f"\n📊 合計 {len(chunks)} ファイルに分割します")
            set_progress(75)

            # Step 3: 出力
            output_files = []
            for i, (start, end) in enumerate(chunks, 1):
                out_name = f"{prefix}_{i:03d}.pdf"
                out_path = output_dir / out_name
                extract_pages(merged, str(out_path), start, end)
                sz = os.path.getsize(out_path)
                log(f"💾 [{i}/{len(chunks)}] {out_name} — {sz/1024/1024:.1f} MB (p{start}–p{end})")
                output_files.append(out_name)
                set_progress(75 + int((i / len(chunks)) * 24))

        with jobs_lock:
            jobs[job_id]["output_files"] = output_files
            jobs[job_id]["status"] = "done"
            jobs[job_id]["progress"] = 100
            jobs[job_id]["log"].append(f"\n✅ 完了！ {len(output_files)} ファイルを生成しました")

    except Exception as e:
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["log"].append(f"❌ エラー: {e}")

    finally:
        # アップロードファイルを削除
        for p in input_paths:
            try:
                os.remove(p)
            except Exception:
                pass


# ─── ルート ───────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/split", methods=["POST"])
def api_split():
    files = request.files.getlist("pdfs")
    if not files:
        return jsonify({"error": "ファイルが選択されていません"}), 400

    size_mb = float(request.form.get("size_mb", 200))
    prefix = request.form.get("prefix", "split").strip() or "split"
    limit_bytes = int(size_mb * 1024 * 1024)

    # アップロード保存
    job_id = str(uuid.uuid4())
    upload_dir = UPLOAD_FOLDER / job_id
    upload_dir.mkdir()
    saved_paths = []
    for f in files:
        if not f.filename.lower().endswith(".pdf"):
            continue
        dest = upload_dir / f.filename
        f.save(str(dest))
        saved_paths.append(str(dest))

    if not saved_paths:
        return jsonify({"error": "PDFファイルが見つかりません"}), 400

    # 出力ディレクトリ
    out_dir = OUTPUT_FOLDER / job_id
    out_dir.mkdir()

    # ジョブ登録
    with jobs_lock:
        jobs[job_id] = {
            "status": "running",
            "progress": 0,
            "log": [],
            "output_files": [],
            "output_dir": str(out_dir),
            "created_at": time.time(),
        }

    # バックグラウンドで実行
    t = threading.Thread(
        target=split_worker,
        args=(job_id, saved_paths, limit_bytes, prefix, out_dir),
        daemon=True,
    )
    t.start()

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def api_status(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "ジョブが見つかりません"}), 404
    return jsonify({
        "status": job["status"],
        "progress": job["progress"],
        "log": job["log"],
        "output_files": job["output_files"],
    })


@app.route("/api/download/<job_id>/<filename>")
def api_download_file(job_id: str, filename: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return "Not found", 404
    path = Path(job["output_dir"]) / filename
    if not path.exists():
        return "Not found", 404
    return send_file(str(path), as_attachment=True, download_name=filename)


@app.route("/api/download_zip/<job_id>")
def api_download_zip(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return "Not ready", 400

    out_dir = Path(job["output_dir"])
    zip_path = out_dir / "all_splits.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as zf:
        for fname in job["output_files"]:
            zf.write(out_dir / fname, fname)

    return send_file(str(zip_path), as_attachment=True, download_name="split_pdfs.zip")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
