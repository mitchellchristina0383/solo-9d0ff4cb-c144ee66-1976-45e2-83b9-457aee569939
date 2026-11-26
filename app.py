"""Flask drill page for the ordered parallel-gzip pipeline.

Run locally::

    python app.py
    gunicorn -w 1 --threads 16 --no-control-socket -b 0.0.0.0:8000 app:app

One process / one worker is required: job state lives in memory and the
compression slots are OS threads.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from flask import Flask, jsonify, request, send_file, send_from_directory

from compressor import (
    BLOCK_COUNT,
    BLOCK_SIZE,
    COMPRESS_SLOTS,
    DEFAULT_LEVEL,
    MAX_BASE_DELAY_MS,
    MAX_DELAY_MS,
    MAX_LEVEL,
    MAX_STAGED,
    MIN_LEVEL,
    SAMPLE,
    SAMPLE_SHA256,
    TOTAL_SIZE,
    Conflict,
    JobError,
    JobManager,
    JobSettings,
    NotFound,
)

MAX_SLOW_BLOCKS = COMPRESS_SLOTS + MAX_STAGED

DATA_DIR = Path(os.environ.get("DRILL_DATA_DIR", Path(__file__).parent / "data")).resolve()

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 64 * 1024
app.json.ensure_ascii = False


def _prepare_data_dir(path: Path) -> JobManager:
    """Prepare the data dir for a fresh process.

    Job state lives in memory, so archives from a previous process are
    unreachable (their download URLs no longer resolve).  Clear them at
    startup; this also removes any ``.part`` file left by a killed process.
    """
    jobs_dir = path / "jobs"
    if jobs_dir.exists():
        shutil.rmtree(jobs_dir, ignore_errors=True)
    jobs_dir.mkdir(parents=True, exist_ok=True)
    return JobManager(path)


manager = _prepare_data_dir(DATA_DIR)


# --- validation -------------------------------------------------------------


def _parse_settings(payload: dict) -> JobSettings:
    if not isinstance(payload, dict):
        raise JobError("请求体必须是 JSON 对象")

    level_raw = payload.get("level", DEFAULT_LEVEL)
    if isinstance(level_raw, bool) or not isinstance(level_raw, int):
        raise JobError("compression level 必须是整数")
    if not (MIN_LEVEL <= level_raw <= MAX_LEVEL):
        raise JobError(f"compression level 必须在 {MIN_LEVEL}–{MAX_LEVEL} 之间")

    base_raw = payload.get("base_delay_ms", 0)
    if isinstance(base_raw, bool) or not isinstance(base_raw, int):
        raise JobError("base_delay_ms 必须是整数")
    if not (0 <= base_raw <= MAX_BASE_DELAY_MS):
        raise JobError(f"base_delay_ms 必须在 0–{MAX_BASE_DELAY_MS} 毫秒之间")

    slow_raw = payload.get("slow_blocks") or {}
    if not isinstance(slow_raw, dict):
        raise JobError("slow_blocks 必须是 {块号: 延迟毫秒} 对象")
    slow: dict[int, int] = {}
    for key, value in slow_raw.items():
        try:
            index = int(key)
        except (TypeError, ValueError):
            raise JobError(f"非法块号：{key!r}") from None
        if not (0 <= index < BLOCK_COUNT):
            raise JobError(f"块号必须在 0–{BLOCK_COUNT - 1} 之间")
        if isinstance(value, bool) or not isinstance(value, int):
            raise JobError("延迟必须是整数毫秒")
        if not (0 <= value <= MAX_DELAY_MS):
            raise JobError(f"延迟必须在 0–{MAX_DELAY_MS} 毫秒之间")
        if index in slow:
            raise JobError(f"块 {index} 重复设置")
        if value > 0:
            slow[index] = value
    if len(slow) > MAX_SLOW_BLOCKS:
        raise JobError(f"最多只能拖慢 {MAX_SLOW_BLOCKS} 个块，否则演练失去意义")

    fail_raw = payload.get("fail_write_at")
    fail_at = None
    if isinstance(fail_raw, bool):
        raise JobError("fail_write_at 必须是整数步号或留空")
    if fail_raw not in (None, "", 0, "0"):
        if not isinstance(fail_raw, int):
            raise JobError("fail_write_at 必须是整数步号或留空")
        if not (1 <= fail_raw <= BLOCK_COUNT):
            raise JobError(f"写盘失败步号必须在 1–{BLOCK_COUNT} 之间")
        fail_at = fail_raw

    return JobSettings(
        level=level_raw,
        base_delay_ms=base_raw,
        slow_blocks=slow,
        fail_write_at=fail_at,
    )


# --- pages ------------------------------------------------------------------


@app.get("/")
def index():
    templates_dir = Path(app.root_path) / app.template_folder
    return send_from_directory(templates_dir, "index.html")


@app.get("/api/meta")
def meta():
    return jsonify(
        {
            "block_count": BLOCK_COUNT,
            "block_size": BLOCK_SIZE,
            "total_size": TOTAL_SIZE,
            "compress_slots": COMPRESS_SLOTS,
            "max_staged": MAX_STAGED,
            "level_min": MIN_LEVEL,
            "level_max": MAX_LEVEL,
            "default_level": DEFAULT_LEVEL,
            "sample_sha256": SAMPLE_SHA256,
            "base_delay_max": MAX_BASE_DELAY_MS,
            "data_dir": str(DATA_DIR),
        }
    )


@app.get("/api/sample")
def sample():
    response = app.response_class(SAMPLE, mimetype="application/octet-stream")
    response.headers["Content-Disposition"] = "attachment; filename=backup-sample.bin"
    response.headers["Content-Length"] = str(len(SAMPLE))
    return response


# --- job API ----------------------------------------------------------------


@app.post("/api/jobs")
def create_job():
    payload = request.get_json(silent=True)
    if payload is None:
        payload = {}
    try:
        settings = _parse_settings(payload)
        job = manager.create(settings)
    except JobError as exc:
        status = 409 if isinstance(exc, Conflict) else 400
        return jsonify({"error": str(exc)}), status
    return jsonify(job.snapshot()), 201


@app.get("/api/jobs/<job_id>")
def get_job(job_id: str):
    try:
        job = manager.get(job_id)
    except NotFound as exc:
        return jsonify({"error": str(exc)}), 404
    return jsonify(job.snapshot())


@app.get("/api/jobs/latest")
def latest_job():
    latest = manager.latest()
    return jsonify({"latest": latest.snapshot() if latest else None})


@app.post("/api/jobs/<job_id>/fail-write")
def fail_job_write(job_id: str):
    """Arm a simulated disk failure on the next block write (live lever)."""
    try:
        job = manager.get(job_id)
    except NotFound as exc:
        return jsonify({"error": str(exc)}), 404
    if not job.request_write_failure():
        return jsonify({"error": "作业已结束，无法再触发写盘失败"}), 409
    return jsonify(job.snapshot())


@app.post("/api/jobs/<job_id>/cancel")
def cancel_job(job_id: str):
    try:
        job = manager.get(job_id)
    except NotFound as exc:
        return jsonify({"error": str(exc)}), 404
    job.cancel()
    if not job.wait(timeout=5):
        return jsonify({"error": "取消后线程未在 5 秒内退出"}), 500
    return jsonify(job.snapshot())


@app.get("/api/jobs/<job_id>/download")
def download_job(job_id: str):
    try:
        job = manager.get(job_id)
    except NotFound as exc:
        return jsonify({"error": str(exc)}), 404
    if not job.snapshot()["download_ready"]:
        return jsonify({"error": "作业未成功完成，没有可下载文件"}), 409
    path = job.final_path.resolve()
    if path.parent != job.job_dir.resolve() or not path.is_file():
        return jsonify({"error": "产物文件不存在"}), 404
    response = send_file(
        path,
        mimetype="application/gzip",
        as_attachment=True,
        download_name="backup.gz",
        max_age=0,
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Content-Length"] = str(path.stat().st_size)
    return response


@app.errorhandler(JobError)
def handle_job_error(exc: JobError):
    return jsonify({"error": str(exc)}), 400


@app.errorhandler(404)
def not_found(_exc):
    return jsonify({"error": "not found"}), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), threaded=True)
