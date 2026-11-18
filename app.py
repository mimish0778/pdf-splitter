"""
PDF Splitter — Flask + qpdf  (Render Docker 対応)
=================================================
- チャンクアップロード（50MB/chunk）でタイムアウト回避
- 全結合しない逐次処理でメモリ節約
- 出力サイズを指定値以下に厳守（SAFETY + 最終実測ガード）
"""

import json
import math
import os
import re
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

CHUNK_MB = 50    # ブラウザが 1 回に送るチャンクの大きさ（templates/index.html の CHUNK_SIZE と合わせる）
# 1 回のリクエストの上限。チャンク本体にフォームの項目の分を足した大きさ
app.config["MAX_CONTENT_LENGTH"] = (CHUNK_MB + 1) * 1024 * 1024
# 1 つのセッションでアップロードできる合計サイズ。ディスクを使い切られないようにする
MAX_UPLOAD_BYTES = int(float(os.environ.get("MAX_UPLOAD_MB", 2048)) * 1024 * 1024)

# アップロードするファイル名。ブラウザは英数字と . _ - だけにした名前を送ってくる
FILENAME_RE = re.compile(r"[A-Za-z0-9._-]{1,200}")


def valid_session_id(sid: str) -> bool:
    """session_id はブラウザが crypto.randomUUID() で作る UUID だけを受け付ける。
    パスの一部に使うため、"../" や絶対パスで作業ディレクトリの外に出られないようにする。"""
    try:
        return str(uuid.UUID(sid)) == sid.lower()
    except ValueError:
        return False


def valid_filename(name: str) -> bool:
    return bool(FILENAME_RE.fullmatch(name)) and name.strip(".") != ""


def dir_size(path: Path) -> int:
    total = 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except FileNotFoundError:
            pass
    return total


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


def last_modified(path: Path) -> float:
    """ディレクトリの中で最後に更新されたファイルの時刻（中身がなければディレクトリ自体の時刻）。"""
    latest = path.stat().st_mtime
    for p in path.rglob("*"):
        try:
            latest = max(latest, p.stat().st_mtime)
        except FileNotFoundError:
            pass
    return latest


def cleanup_old_jobs():
    now = time.time()
    with jobs_lock:
        expired = [jid for jid, j in jobs.items()
                   if now - j.get("created_at", now) > JOB_TTL]
    for jid in expired:
        with jobs_lock:
            jobs.pop(jid, None)
        shutil.rmtree(WORK_ROOT / jid, ignore_errors=True)

    # 分割を実行しなかったアップロード（途中でタブを閉じた場合など）や、サーバーの再起動で
    # jobs から消えたジョブの作業ディレクトリも、一定時間更新がなければ削除する
    with jobs_lock:
        known = set(jobs)
    for d in WORK_ROOT.iterdir():
        if not d.is_dir() or d.name in known:
            continue
        try:
            if now - last_modified(d) > JOB_TTL:
                shutil.rmtree(d, ignore_errors=True)
        except FileNotFoundError:
            pass


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

    except Exception:
        app.logger.exception("split job %s failed", job_id)
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["log"].append("❌ エラー: 分割に失敗しました。PDF が壊れていないか確認してください")

    finally:
        for p in pdf_paths:
            Path(p).unlink(missing_ok=True)
        shutil.rmtree(work_dir, ignore_errors=True)
        cleanup_old_jobs()


# ─── Flask ルート ─────────────────────────────────────────────

@app.errorhandler(413)
def too_large(_e):
    return jsonify({"error": f"1 回に送れるのは {CHUNK_MB} MB までです"}), 413


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/upload_chunk", methods=["POST"])
def upload_chunk():
    """50MB ずつチャンクでアップロードを受け取る。"""
    cleanup_old_jobs()
    sid          = request.form.get("session_id", "")
    filename     = request.form.get("filename", "file.pdf")
    data         = request.files.get("data")
    try:
        chunk_index  = int(request.form.get("chunk_index", 0))
        total_chunks = int(request.form.get("total_chunks", 1))
    except ValueError:
        return jsonify({"error": "chunk_index と total_chunks は整数で指定してください"}), 400

    if not sid or not data:
        return jsonify({"error": "パラメータ不足"}), 400
    if chunk_index < 0 or total_chunks < 1 or chunk_index >= total_chunks:
        return jsonify({"error": "chunk_index が範囲外です"}), 400
    if not valid_session_id(sid):
        return jsonify({"error": "session_id が不正です"}), 400
    if not valid_filename(filename):
        return jsonify({"error": "ファイル名は英数字と . _ - だけで指定してください"}), 400

    upload_dir = WORK_ROOT / sid / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    dest = upload_dir / filename
    chunk = data.read()
    existing = dir_size(upload_dir)
    if chunk_index == 0 and dest.exists():
        existing -= fsize(dest)  # 同じファイルを最初から送り直す場合は上書きになる
    if existing + len(chunk) > MAX_UPLOAD_BYTES:
        return jsonify({"error": f"アップロードできる合計サイズ（{MAX_UPLOAD_BYTES // 1024 // 1024} MB）を超えています"}), 413

    mode = "ab" if chunk_index > 0 else "wb"
    with open(dest, mode) as f:
        f.write(chunk)

    return jsonify({"received": chunk_index + 1, "done": chunk_index == total_chunks - 1})


@app.route("/api/finalize", methods=["POST"])
def finalize():
    """全チャンク受信後に分割ジョブを開始する。"""
    sid        = request.form.get("session_id", "")
    # 出力ファイル名に使うので、ディレクトリ部分を取り除く
    prefix     = Path(request.form.get("prefix", "split").strip()).name or "split"
    try:
        size_mb    = float(request.form.get("size_mb", 200))
        file_order = json.loads(request.form.get("file_order", "[]"))
    except ValueError:  # json.JSONDecodeError も ValueError の一種
        return jsonify({"error": "size_mb または file_order の形式が正しくありません"}), 400
    if not math.isfinite(size_mb) or size_mb <= 0:
        return jsonify({"error": "size_mb には 0 より大きい数を指定してください"}), 400
    if not isinstance(file_order, list) or not all(isinstance(fn, str) for fn in file_order):
        return jsonify({"error": "file_order はファイル名の配列で指定してください"}), 400

    if not sid:
        return jsonify({"error": "session_id がありません"}), 400
    if not valid_session_id(sid):
        return jsonify({"error": "session_id が不正です"}), 400

    upload_dir = WORK_ROOT / sid / "uploads"
    if not upload_dir.exists():
        return jsonify({"error": "アップロードデータが見つかりません"}), 404

    pdf_paths = (
        [str(upload_dir / fn) for fn in dict.fromkeys(file_order)  # 同じ名前は 1 回だけ
         if valid_filename(fn) and (upload_dir / fn).exists()]
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
