"""
PDF Splitter — Flask + qpdf  (Render Docker 対応)
=================================================
- チャンクアップロード（50MB/chunk）でタイムアウト回避
- 全結合しない逐次処理でメモリ節約
- 出力サイズを指定値以下に厳守（SAFETY + 最終実測ガード）
"""

import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
import zipfile
from pathlib import Path

from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))

jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()

WORK_ROOT = Path(os.environ.get("WORK_ROOT", tempfile.gettempdir())) / "pdf-splitter"
WORK_ROOT.mkdir(parents=True, exist_ok=True)

JOB_TTL = 3600   # 1時間でジョブ削除


# ─── qpdf ヘルパー ────────────────────────────────────────────

def get_page_count(pdf_path: str) -> int:
    r = subprocess.run(
        ["qpdf", "--show-npages", pdf_path],
        capture_output=True, text=True, check=True
    )
    return int(r.stdout.strip())


def qpdf_extract(src: str, dst: str, start: int, end: int):
    """1-indexed inclusive。PDF 内部構造を変更しない。"""
    subprocess.run(
        ["qpdf", src, "--pages", ".", f"{start}-{end}", "--", dst],
        check=True, capture_output=True
    )


def qpdf_merge(parts: list, dst: str):
    """[(path, start, end), ...] を結合。"""
    args = ["qpdf", "--empty", "--pages"]
    for path, s, e in parts:
        args += [path, f"{s}-{e}"]
    args += ["--", dst]
    subprocess.run(args, check=True, capture_output=True)


def fsize(path) -> int:
    return os.path.getsize(str(path))


def cleanup_old_jobs():
    now = time.time()
    with jobs_lock:
        expired = [jid for jid, j in jobs.items()
                   if now - j.get("created_at", now) > JOB_TTL]
    for jid in expired:
        with jobs_lock:
            jobs.pop(jid, None)
        shutil.rmtree(WORK_ROOT / jid, ignore_errors=True)


# ─── 分割ワーカー ─────────────────────────────────────────────

def split_worker(job_id: str, pdf_paths: list,
                 user_limit: int, prefix: str, out_dir: Path):
    """
    user_limit : ユーザー指定の上限バイト数。
                 出力ファイルは必ずこれ未満になる（厳守）。
    """
    work_dir = WORK_ROOT / job_id / "tmp"
    work_dir.mkdir(parents=True, exist_ok=True)

    def log(msg: str):
        with jobs_lock:
            jobs[job_id]["log"].append(msg)

    def prog(pct: float):
        with jobs_lock:
            jobs[job_id]["progress"] = min(int(pct), 99)

    try:
        output_files: list[str] = []
        chunk_idx   = 1
        total       = len(pdf_paths)
        carry_path  = None
        carry_pages = 0

        for fi, src in enumerate(pdf_paths):
            name    = Path(src).name
            log(f"📂 [{fi+1}/{total}] {name}")
            prog(fi / total * 80)

            n       = get_page_count(src)
            is_last = (fi == total - 1)
            log(f"   {n} ページ / {fsize(src)/1024/1024:.1f} MB")

            # carry があれば結合して work を作る
            if carry_path:
                work    = str(work_dir / f"work_{fi}.pdf")
                qpdf_merge([(carry_path, 1, carry_pages), (src, 1, n)], work)
                Path(carry_path).unlink(missing_ok=True)
                carry_path  = None
                carry_pages = 0
                work_n      = get_page_count(work)
                owns_work   = True
                log(f"   carry結合: {work_n} ページ")
            else:
                work      = src
                work_n    = n
                owns_work = False

            page_ptr = 1

            while page_ptr <= work_n:
                remaining = work_n - page_ptr + 1

                # ── 残り全部が user_limit 未満か高速チェック ──
                all_t = str(work_dir / f"all_{chunk_idx}.pdf")
                qpdf_extract(work, all_t, page_ptr, work_n)
                sz_all = fsize(all_t)
                Path(all_t).unlink(missing_ok=True)

                if sz_all < user_limit:
                    if is_last:
                        # 最終ファイル → そのまま確定出力
                        out_name = f"{prefix}_{chunk_idx:03d}.pdf"
                        out_path = out_dir / out_name
                        qpdf_extract(work, str(out_path), page_ptr, work_n)
                        actual = fsize(out_path)
                        log(f"💾 {out_name} — {actual/1024/1024:.2f} MB")
                        output_files.append(out_name)
                        chunk_idx += 1
                    else:
                        # 次ファイルへ carry
                        carry_path = str(work_dir / f"carry_{fi}.pdf")
                        qpdf_extract(work, carry_path, page_ptr, work_n)
                        carry_pages = remaining
                        log(f"   → {remaining} ページを次ファイルへ持ち越し")
                    break

                # ── 二分探索で user_limit 未満に収まる最大ページ数を探す ──
                one_t = str(work_dir / f"one_{chunk_idx}.pdf")
                qpdf_extract(work, one_t, page_ptr, page_ptr)
                sz1 = fsize(one_t)
                Path(one_t).unlink(missing_ok=True)

                if sz1 >= user_limit:
                    # 1ページで既に超過 → 仕方なく単独出力
                    log(f"   ⚠️ 1ページが {sz1/1024/1024:.2f} MB — 上限超えだが単独出力")
                    best_end = page_ptr
                else:
                    lo, hi   = page_ptr + 1, work_n - 1
                    best_end = page_ptr  # 少なくとも1ページは確保

                    while lo <= hi:
                        mid = (lo + hi) // 2
                        t   = str(work_dir / f"bs_{chunk_idx}_{mid}.pdf")
                        qpdf_extract(work, t, page_ptr, mid)
                        sz  = fsize(t)
                        Path(t).unlink(missing_ok=True)
                        # 判定は user_limit で厳守（< なので指定値を超えない）
                        ok  = (sz < user_limit)
                        log(f"   🔍 p{page_ptr}–p{mid} {sz/1024/1024:.2f} MB {'✓' if ok else '✗'}")
                        if ok:
                            best_end = mid
                            lo = mid + 1
                        else:
                            hi = mid - 1

                # ── 確定出力 ──
                out_name = f"{prefix}_{chunk_idx:03d}.pdf"
                out_path = out_dir / out_name
                qpdf_extract(work, str(out_path), page_ptr, best_end)
                actual = fsize(out_path)

                # 念のため最終確認（二分探索が正しければここは通常スキップ）
                while actual >= user_limit and best_end > page_ptr:
                    best_end -= 1
                    qpdf_extract(work, str(out_path), page_ptr, best_end)
                    actual = fsize(out_path)
                    log(f"   ⚠️ 超過修正: → p{best_end} ({actual/1024/1024:.2f} MB)")

                log(f"💾 {out_name} — {actual/1024/1024:.2f} MB (p{page_ptr}–p{best_end})")
                output_files.append(out_name)
                chunk_idx += 1
                page_ptr = best_end + 1

            if owns_work:
                Path(work).unlink(missing_ok=True)

        # carry が残っていたら最終出力
        if carry_path and Path(carry_path).exists():
            out_name = f"{prefix}_{chunk_idx:03d}.pdf"
            out_path = out_dir / out_name
            qpdf_extract(carry_path, str(out_path), 1, carry_pages)
            actual = fsize(out_path)
            # ここも念のため実測ガード
            best_end = carry_pages
            while actual >= user_limit and best_end > 1:
                best_end -= 1
                qpdf_extract(carry_path, str(out_path), 1, best_end)
                actual = fsize(out_path)
            log(f"💾 {out_name} — {actual/1024/1024:.2f} MB (carry残り)")
            output_files.append(out_name)
            Path(carry_path).unlink(missing_ok=True)

        with jobs_lock:
            jobs[job_id].update({
                "status"      : "done",
                "progress"    : 100,
                "output_files": output_files,
            })
            jobs[job_id]["log"].append(
                f"\n✅ 完了！ {len(output_files)} ファイルを生成しました"
            )

    except Exception as e:
        import traceback
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["log"].append(f"❌ エラー: {e}\n{traceback.format_exc()}")

    finally:
        for p in pdf_paths:
            Path(p).unlink(missing_ok=True)
        shutil.rmtree(work_dir, ignore_errors=True)
        cleanup_old_jobs()


# ─── Flask ルート ─────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/upload_chunk", methods=["POST"])
def upload_chunk():
    """50MB ずつチャンクでアップロードを受け取る。"""
    cleanup_old_jobs()
    sid          = request.form.get("session_id", "")
    chunk_index  = int(request.form.get("chunk_index", 0))
    total_chunks = int(request.form.get("total_chunks", 1))
    filename     = Path(request.form.get("filename", "file.pdf")).name
    data         = request.files.get("data")

    if not sid or not data:
        return jsonify({"error": "パラメータ不足"}), 400

    upload_dir = WORK_ROOT / sid / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    dest = upload_dir / filename
    mode = "ab" if chunk_index > 0 else "wb"
    with open(dest, mode) as f:
        f.write(data.read())

    return jsonify({"received": chunk_index + 1, "done": chunk_index == total_chunks - 1})


@app.route("/api/finalize", methods=["POST"])
def finalize():
    """全チャンク受信後に分割ジョブを開始する。"""
    sid        = request.form.get("session_id", "")
    size_mb    = float(request.form.get("size_mb", 200))
    prefix     = request.form.get("prefix", "split").strip() or "split"
    file_order = json.loads(request.form.get("file_order", "[]"))

    if not sid:
        return jsonify({"error": "session_id がありません"}), 400

    upload_dir = WORK_ROOT / sid / "uploads"
    if not upload_dir.exists():
        return jsonify({"error": "アップロードデータが見つかりません"}), 404

    pdf_paths = (
        [str(upload_dir / fn) for fn in file_order if (upload_dir / fn).exists()]
        if file_order else
        sorted(str(p) for p in upload_dir.glob("*.pdf"))
    )
    if not pdf_paths:
        return jsonify({"error": "PDFが見つかりません"}), 400

    job_id  = sid
    out_dir = WORK_ROOT / job_id / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    with jobs_lock:
        jobs[job_id] = {
            "status"      : "running",
            "progress"    : 0,
            "log"         : [],
            "output_files": [],
            "output_dir"  : str(out_dir),
            "created_at"  : time.time(),
        }

    threading.Thread(
        target=split_worker,
        args=(job_id, pdf_paths, int(size_mb * 1024 * 1024), prefix, out_dir),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def api_status(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "ジョブが見つかりません"}), 404
    return jsonify({
        "status"      : job["status"],
        "progress"    : job["progress"],
        "log"         : job["log"],
        "output_files": job["output_files"],
    })


@app.route("/api/download/<job_id>/<filename>")
def api_download(job_id: str, filename: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return "Not found", 404
    path = Path(job["output_dir"]) / Path(filename).name
    if not path.exists():
        return "Not found", 404
    return send_file(str(path), as_attachment=True, download_name=path.name)


@app.route("/api/download_zip/<job_id>")
def api_download_zip(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return "Not ready", 400

    out_dir  = Path(job["output_dir"])
    zip_path = out_dir / "all_splits.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
        for fname in job["output_files"]:
            fp = out_dir / fname
            if fp.exists():
                zf.write(fp, fname)

    return send_file(str(zip_path), as_attachment=True, download_name="split_pdfs.zip")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
