"""Tests for the ordered parallel-gzip pipeline and the Flask drill page."""

from __future__ import annotations

import gzip
import hashlib
import importlib
import sys
import time
import zlib
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import compressor  # noqa: E402
from compressor import (  # noqa: E402
    BLOCK_COUNT,
    MAX_STAGED,
    SAMPLE,
    JobManager,
    JobSettings,
    run_job,
)


@pytest.fixture()
def workdir(tmp_path):
    return tmp_path


def decompressed(path: Path) -> bytes:
    return gzip.decompress(path.read_bytes())


# ---------------------------------------------------------------------------
# pipeline correctness
# ---------------------------------------------------------------------------


def test_sample_shape():
    assert len(SAMPLE) == compressor.TOTAL_SIZE
    assert len(compressor.BLOCKS) == BLOCK_COUNT
    assert hashlib.sha256(SAMPLE).hexdigest() == compressor.SAMPLE_SHA256


def test_full_run_roundtrip_and_order(workdir):
    job = run_job(JobSettings(level=6), workdir)
    snap = job.snapshot()

    assert snap["state"] == "complete", snap["error"]
    assert snap["written_order"] == list(range(BLOCK_COUNT))
    assert snap["download_ready"] is True
    assert decompressed(job.final_path) == SAMPLE
    assert snap["crc32"] == f"{zlib.crc32(SAMPLE) & 0xFFFFFFFF:08x}"
    assert snap["staging_peak"] <= MAX_STAGED
    # A single gzip member: one trailer whose ISIZE covers the whole sample.
    raw = job.final_path.read_bytes()
    assert int.from_bytes(raw[-4:], "little") == len(SAMPLE)
    assert raw[:2] == b"\x1f\x8b"
    assert not job.partial_path.exists()


def test_all_compressed_in_parallel_but_written_in_order(workdir):
    job = run_job(JobSettings(level=1), workdir)
    snap = job.snapshot()
    assert snap["state"] == "complete"
    assert snap["written_order"] == list(range(BLOCK_COUNT))
    assert decompressed(job.final_path) == SAMPLE


def test_staging_peak_never_exceeds_two_across_runs(workdir):
    for delay in (0, 0, 20, 50):
        job = run_job(JobSettings(level=6, slow_blocks={5: delay}), workdir)
        snap = job.snapshot()
        assert snap["state"] == "complete"
        assert snap["staging_peak"] <= MAX_STAGED
        for event in snap["write_events"]:
            assert event["staged_at_write"] <= MAX_STAGED
        assert decompressed(job.final_path) == SAMPLE


# ---------------------------------------------------------------------------
# slow block: later blocks finish first, writer stays ordered
# ---------------------------------------------------------------------------


def test_slow_block_finishes_late_but_is_written_in_order(workdir):
    job = run_job(JobSettings(level=6, slow_blocks={3: 900}), workdir)
    snap = job.snapshot()

    assert snap["state"] == "complete", snap["error"]
    assert snap["compressed_order"][:5] == [0, 1, 2, 4, 3]
    assert snap["written_order"] == list(range(BLOCK_COUNT))
    # Block 4 was compressed before block 3 but written after it.
    rank_4 = snap["compressed_rank"]["4"]
    rank_3 = snap["compressed_rank"]["3"]
    assert rank_4 < rank_3
    # The writer never skipped ahead.
    assert snap["written_order"] == sorted(snap["written_order"])
    assert snap["staging_peak"] == MAX_STAGED
    assert decompressed(job.final_path) == SAMPLE


def test_slow_block_does_not_let_later_blocks_pass_writer(workdir):
    """While block 2 is slow, block 3 is staged; block 2 still goes first."""
    job = run_job(JobSettings(level=6, slow_blocks={2: 700}), workdir)
    snap = job.snapshot()
    assert snap["state"] == "complete"
    assert snap["compressed_order"].index(3) < snap["compressed_order"].index(2)
    # Block 3's compression happened before block 2's write completed.
    three = snap["blocks"][3]
    two = snap["blocks"][2]
    assert three["status"] == "written"
    assert snap["written_order"] == list(range(BLOCK_COUNT))
    assert snap["staging_peak"] <= MAX_STAGED
    assert decompressed(job.final_path) == SAMPLE


# ---------------------------------------------------------------------------
# write failure
# ---------------------------------------------------------------------------


def test_write_failure_aborts_and_leaves_no_artifact(workdir):
    job = run_job(JobSettings(level=6, slow_blocks={2: 200}, fail_write_at=5), workdir)
    snap = job.snapshot()

    assert snap["state"] == "failed"
    assert snap["written_order"] == [0, 1, 2, 3]
    assert "写盘失败" in snap["error"]
    assert snap["download_ready"] is False
    assert snap["download_url"] is None
    assert not job.final_path.exists()
    assert not job.partial_path.exists()
    assert list(job.job_dir.iterdir()) == []
    # Staging was dropped.
    assert snap["staging_now"] == 0


def test_write_failure_on_first_block(workdir):
    job = run_job(JobSettings(level=6, fail_write_at=1), workdir)
    snap = job.snapshot()
    assert snap["state"] == "failed"
    assert snap["written_order"] == []
    assert snap["compressed_count"] <= MAX_STAGED
    assert not job.final_path.exists() and not job.partial_path.exists()


def test_previous_artifact_is_removed_on_failure(workdir):
    ok = run_job(JobSettings(level=6), workdir)
    assert ok.final_path.exists()
    # Reuse the same job directory for a failing run.
    failed = compressor.Job(ok.id, JobSettings(level=6, fail_write_at=2), workdir)
    failed.start()
    failed.wait(timeout=60)
    snap = failed.snapshot()
    assert snap["state"] == "failed"
    assert not failed.final_path.exists()
    assert not failed.partial_path.exists()


# ---------------------------------------------------------------------------
# cancellation
# ---------------------------------------------------------------------------


def test_cancel_releases_staging_and_artifact(workdir):
    manager = JobManager(workdir)
    job = manager.create(JobSettings(level=6, slow_blocks={0: 2000}))
    time.sleep(0.15)
    job.cancel()
    assert job.wait(timeout=10) is True
    snap = job.snapshot()
    assert snap["state"] == "cancelled"
    assert snap["download_ready"] is False
    assert not job.final_path.exists() and not job.partial_path.exists()
    assert snap["staging_now"] == 0


# ---------------------------------------------------------------------------
# Flask API
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(tmp_path, monkeypatch):
    import app as app_module

    monkeypatch.setenv("DRILL_DATA_DIR", str(tmp_path / "data"))
    importlib.reload(app_module)
    app_module.app.config.update(TESTING=True)
    with app_module.app.test_client() as test_client:
        yield test_client
    # Let any background threads finish/abort before tmp cleanup.
    if app_module.manager.latest():
        app_module.manager.latest().wait(timeout=10)


def _wait_for(client, job_id, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payload = client.get(f"/api/jobs/{job_id}").get_json()
        if payload["state"] != "running":
            return payload
        time.sleep(0.05)
    raise AssertionError("job did not finish")


def test_api_meta_and_sample(client):
    meta = client.get("/api/meta").get_json()
    assert meta["block_count"] == BLOCK_COUNT
    assert meta["max_staged"] == MAX_STAGED
    assert meta["compress_slots"] == 2
    sample = client.get("/api/sample")
    assert sample.status_code == 200
    assert sample.data == SAMPLE


def test_api_page_loads(client):
    page = client.get("/")
    assert page.status_code == 200
    body = page.data.decode("utf-8")
    assert "暂存峰值" in body
    assert "下载 backup.gz" in body


def test_api_full_job_download_roundtrip(client):
    created = client.post("/api/jobs", json={"level": 6})
    assert created.status_code == 201, created.get_json()
    job_id = created.get_json()["job_id"]

    snap = _wait_for(client, job_id)
    assert snap["state"] == "complete"
    assert snap["written_order"] == list(range(BLOCK_COUNT))
    assert snap["staging_peak"] <= MAX_STAGED

    download = client.get(f"/api/jobs/{job_id}/download")
    assert download.status_code == 200
    assert download.headers["Content-Type"] == "application/gzip"
    assert gzip.decompress(download.data) == SAMPLE


def test_api_write_failure_returns_no_download(client):
    created = client.post(
        "/api/jobs", json={"level": 6, "fail_write_at": 4, "slow_blocks": {"1": 300}}
    )
    assert created.status_code == 201
    job_id = created.get_json()["job_id"]

    snap = _wait_for(client, job_id)
    assert snap["state"] == "failed"
    assert "写盘失败" in snap["error"]
    assert snap["download_ready"] is False

    download = client.get(f"/api/jobs/{job_id}/download")
    assert download.status_code == 409
    assert download.is_json
    assert "没有可下载文件" in download.get_json()["error"]


def test_api_live_fail_write_trigger(client):
    created = client.post(
        "/api/jobs", json={"level": 6, "base_delay_ms": 200}
    )
    assert created.status_code == 201
    job_id = created.get_json()["job_id"]

    # Let a block or two go through, then arm the failure.
    time.sleep(0.05)
    armed = client.post(f"/api/jobs/{job_id}/fail-write")
    assert armed.status_code == 200
    assert armed.get_json()["fail_next_armed"] is True

    snap = _wait_for(client, job_id)
    assert snap["state"] == "failed"
    assert "手动触发" in snap["error"]
    assert snap["download_ready"] is False
    assert client.get(f"/api/jobs/{job_id}/download").status_code == 409
    job = __import__("app").manager.get(job_id)
    assert not job.final_path.exists() and not job.partial_path.exists()

    # Arming after the job is over is rejected.
    assert client.post(f"/api/jobs/{job_id}/fail-write").status_code == 409


def test_api_conflict_when_job_running(client):
    first = client.post("/api/jobs", json={"slow_blocks": {"0": 1200}})
    assert first.status_code == 201
    second = client.post("/api/jobs", json={})
    assert second.status_code == 409
    assert "正在运行" in second.get_json()["error"]
    job_id = first.get_json()["job_id"]
    client.post(f"/api/jobs/{job_id}/cancel")
    _wait_for(client, job_id)


@pytest.mark.parametrize(
    "payload, fragment",
    [
        ({"level": 0}, "compression level"),
        ({"level": 10}, "compression level"),
        ({"level": "6"}, "compression level"),
        ({"slow_blocks": {"99": 100}}, "块号"),
        ({"slow_blocks": {"1": -5}}, "延迟"),
        ({"slow_blocks": {"1": 99999}}, "延迟"),
        ({"slow_blocks": {"1": 100, "2": 100, "3": 100, "4": 100, "5": 100}}, "最多"),
        ({"fail_write_at": 0}, None),
        ({"fail_write_at": 17}, "步号"),
        ({"fail_write_at": "x"}, "步号"),
        ({"fail_write_at": True}, "步号"),
    ],
)
def test_api_validation(client, payload, fragment):
    response = client.post("/api/jobs", json=payload)
    if fragment is None:
        # 0 is treated as "no failure" and is accepted.
        assert response.status_code == 201
        client.post(f"/api/jobs/{response.get_json()['job_id']}/cancel")
        return
    assert response.status_code == 400, response.get_json()
    assert fragment in response.get_json()["error"]


def test_api_unknown_job(client):
    assert client.get("/api/jobs/nope").status_code == 404
    assert client.get("/api/jobs/nope/download").status_code == 404
    assert client.post("/api/jobs/nope/cancel").status_code == 404
