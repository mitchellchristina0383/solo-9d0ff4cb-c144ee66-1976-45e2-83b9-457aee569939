"""Tests for the ordered parallel-gzip pipeline and the Flask drill page."""

from __future__ import annotations

import gzip
import hashlib
import importlib
import pathlib
import sys
import threading
import time
import zlib
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import compressor  # noqa: E402
from compressor import (  # noqa: E402
    BLOCK_COUNT,
    COMPRESS_SLOTS,
    MAX_STAGED,
    SAMPLE,
    Conflict,
    JobManager,
    JobSettings,
    JobStartError,
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
# atomic start: directory / thread startup failures must roll back cleanly
# ---------------------------------------------------------------------------


def test_start_mkdir_failure_rolls_back_and_is_retryable(workdir, monkeypatch):
    real_mkdir = pathlib.Path.mkdir

    def boom(self, *args, **kwargs):
        if "jobs" in self.parts:
            raise OSError("模拟 EACCES：建目录失败")
        return real_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "mkdir", boom)
    manager = JobManager(workdir)
    with pytest.raises(JobStartError, match="创建作业目录失败"):
        manager.create(JobSettings(level=6))

    # The failed attempt never reached the registry: it is not queryable and
    # "latest" is unchanged, so a retry cannot be blocked by a phantom job.
    assert manager._jobs == {}
    assert manager.latest() is None

    jobs_root = workdir / "jobs"
    assert not jobs_root.exists() or list(jobs_root.iterdir()) == []

    # Same request retried once the disk condition is fixed.
    monkeypatch.undo()
    job = manager.create(JobSettings(level=6))
    assert job.wait(timeout=30) is True
    snap = job.snapshot()
    assert snap["state"] == "complete"
    assert snap["download_ready"] is True
    assert decompressed(job.final_path) == SAMPLE


def test_partial_thread_start_failure_stops_threads_and_wipes_artifact(
    workdir, monkeypatch
):
    manager = JobManager(workdir)
    real_start = threading.Thread.start
    spawned: list[threading.Thread] = []

    def flaky_start(self):
        # The second compression slot never comes up; the writer and slot 0
        # are already running by then.
        if self.name.startswith(f"compress-{COMPRESS_SLOTS - 1}-"):
            raise RuntimeError("模拟线程启动失败（只起来部分线程）")
        spawned.append(self)
        return real_start(self)

    monkeypatch.setattr(threading.Thread, "start", flaky_start)
    with pytest.raises(JobStartError, match="作业线程启动失败"):
        manager.create(JobSettings(level=6, base_delay_ms=50))

    # Not registered => GET /api/jobs/<id> would 404; no partial backup can
    # ever become downloadable.
    assert manager._jobs == {}
    assert manager.latest() is None

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and any(t.is_alive() for t in spawned):
        time.sleep(0.05)
    assert all(not t.is_alive() for t in spawned)

    # Everything this attempt put on disk is gone (job dir incl. .part/.gz).
    jobs_root = workdir / "jobs"
    assert list(jobs_root.rglob("*")) == []

    # Retry with healthy threads: full pipeline runs and is downloadable.
    monkeypatch.undo()
    job = manager.create(JobSettings(level=6))
    assert job.wait(timeout=30) is True
    snap = job.snapshot()
    assert snap["state"] == "complete"
    assert len(job.threads) == COMPRESS_SLOTS + 1
    assert all(not t.is_alive() for t in job.threads)
    assert job.final_path.is_file()
    assert decompressed(job.final_path) == SAMPLE


def test_second_create_during_startup_gets_conflict_but_old_backup_stays_served(
    workdir, monkeypatch
):
    manager = JobManager(workdir)
    old_job = manager.create(JobSettings(level=6))
    assert old_job.wait(timeout=30) is True

    gate_held = threading.Event()
    release = threading.Event()
    real_start = compressor.Job.start

    def slow_start(self):
        gate_held.set()
        assert release.wait(10)
        return real_start(self)

    monkeypatch.setattr(compressor.Job, "start", slow_start)
    holder_error: list[Exception] = []

    def hold_gate():
        try:
            manager.create(JobSettings(level=6))
        except Exception as exc:  # pragma: no cover - surfaced below
            holder_error.append(exc)

    holder = threading.Thread(target=hold_gate)
    holder.start()
    assert gate_held.wait(10)
    time.sleep(0.1)
    try:
        # Another order while the first is in its start/rollback window.
        with pytest.raises(Conflict):
            manager.create(JobSettings(level=6))
        # Reads and downloads never take the creation gate.
        snap = manager.get(old_job.id).snapshot()
        assert snap["state"] == "complete" and snap["download_ready"] is True
        assert old_job.final_path.is_file()
    finally:
        release.set()
        holder.join(15)

    assert holder_error == []
    new_job = manager.latest()
    assert new_job is not old_job
    assert new_job.wait(timeout=30) is True
    assert new_job.snapshot()["state"] == "complete"


def test_failed_start_does_not_touch_latest_or_pruning(workdir):
    manager = JobManager(workdir, keep=2)
    first = manager.create(JobSettings(level=6))
    assert first.wait(timeout=30) is True

    real_mkdir = pathlib.Path.mkdir

    def boom(self, *args, **kwargs):
        if "jobs" in self.parts:
            raise OSError("模拟 EACCES：建目录失败")
        return real_mkdir(self, *args, **kwargs)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(pathlib.Path, "mkdir", boom)
        with pytest.raises(JobStartError):
            manager.create(JobSettings(level=6))

    # "latest" still points at the last *successful* job.
    assert manager.latest().id == first.id

    ids = []
    for _ in range(3):
        job = manager.create(JobSettings(level=6))
        assert job.wait(timeout=30) is True
        ids.append(job.id)
    # Original keep/prune rule preserved: newest two survive.
    assert set(manager._jobs) == set(ids[-2:])


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


def test_api_start_failure_returns_500_and_retry_succeeds(client, monkeypatch):
    import app as app_module

    real_mkdir = pathlib.Path.mkdir

    def boom(self, *args, **kwargs):
        if "jobs" in self.parts:
            raise OSError("模拟 EACCES：建目录失败")
        return real_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "mkdir", boom)
    failed = client.post("/api/jobs", json={"level": 6})
    assert failed.status_code == 500
    assert "创建作业目录失败" in failed.get_json()["error"]

    # Nothing was registered: the phantom id is unknown and latest is empty.
    assert client.get("/api/jobs/latest").get_json()["latest"] is None
    monkeypatch.undo()

    # The very next order on the same page goes through.
    retried = client.post("/api/jobs", json={"level": 6})
    assert retried.status_code == 201, retried.get_json()
    job_id = retried.get_json()["job_id"]
    snap = _wait_for(client, job_id)
    assert snap["state"] == "complete"
    assert client.get(f"/api/jobs/{job_id}/download").status_code == 200


def test_api_partial_thread_start_failure_is_invisible_and_retryable(
    client, monkeypatch
):
    import app as app_module

    real_start = threading.Thread.start

    def flaky_start(self):
        if self.name.startswith(f"compress-{COMPRESS_SLOTS - 1}-"):
            raise RuntimeError("模拟线程启动失败（只起来部分线程）")
        return real_start(self)

    monkeypatch.setattr(threading.Thread, "start", flaky_start)
    failed = client.post("/api/jobs", json={"level": 6})
    assert failed.status_code == 500
    assert "作业线程启动失败" in failed.get_json()["error"]
    monkeypatch.undo()

    # The rolled-back job id must not be discoverable.
    assert client.get("/api/jobs/latest").get_json()["latest"] is None

    retried = client.post("/api/jobs", json={"level": 6})
    assert retried.status_code == 201
    snap = _wait_for(client, retried.get_json()["job_id"])
    assert snap["state"] == "complete"

    # Only the retried job's directory remains; the failed attempt left none.
    jobs_root = pathlib.Path(app_module.manager.data_dir) / "jobs"
    assert [p.name for p in jobs_root.iterdir()] == [retried.get_json()["job_id"]]
    assert not list(jobs_root.rglob("*.part"))


def test_api_409_while_starting_but_old_backup_still_downloadable(client, monkeypatch):
    old = client.post("/api/jobs", json={"level": 6})
    assert old.status_code == 201
    old_id = old.get_json()["job_id"]
    assert _wait_for(client, old_id)["state"] == "complete"

    import app as app_module

    gate_held = threading.Event()
    release = threading.Event()
    real_start = compressor.Job.start

    def slow_start(self):
        gate_held.set()
        assert release.wait(10)
        return real_start(self)

    monkeypatch.setattr(compressor.Job, "start", slow_start)
    holder_error: list[Exception] = []

    def hold_gate():
        # Drive the manager directly: the Flask test client executes requests
        # inline in the calling thread, which would deadlock against release.
        try:
            app_module.manager.create(JobSettings(level=6))
        except Exception as exc:  # pragma: no cover - surfaced below
            holder_error.append(exc)

    holder = threading.Thread(target=hold_gate)
    holder.start()
    assert gate_held.wait(10)
    time.sleep(0.1)
    try:
        conflict = client.post("/api/jobs", json={"level": 6})
        assert conflict.status_code == 409
        # Old backup remains fully available during the start window.
        old_snap = client.get(f"/api/jobs/{old_id}").get_json()
        assert old_snap["download_ready"] is True
        assert client.get(f"/api/jobs/{old_id}/download").status_code == 200
    finally:
        release.set()
        holder.join(15)

    assert holder_error == []
    new_job = app_module.manager.latest()
    assert new_job is not app_module.manager.get(old_id)
    assert new_job.wait(timeout=30) is True
    assert new_job.snapshot()["state"] == "complete"


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
