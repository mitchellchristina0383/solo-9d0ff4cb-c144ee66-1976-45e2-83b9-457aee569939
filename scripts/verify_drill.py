#!/usr/bin/env python3
"""End-to-end drill check: slow block and simulated write failure.

Runs both scenarios through the Flask routes with an in-process test client
(no server needed) and prints what happened.  Exit code 0 only if every check
passes.

    python scripts/verify_drill.py
"""

from __future__ import annotations

import gzip
import hashlib
import json
import pathlib
import sys
import tempfile
import threading
import time
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as app_module  # noqa: E402
import compressor  # noqa: E402
from compressor import (  # noqa: E402
    BLOCK_COUNT,
    COMPRESS_SLOTS,
    MAX_STAGED,
    SAMPLE,
    JobSettings,
    JobStartError,
)

SAMPLE_SHA = hashlib.sha256(SAMPLE).hexdigest()
failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    if not ok:
        failures.append(label)


def wait(client, job_id: str, timeout: float = 60.0) -> tuple[dict, int]:
    deadline = time.monotonic() + timeout
    observed_peak = 0
    samples = 0
    while time.monotonic() < deadline:
        snapshot = client.get(f"/api/jobs/{job_id}").get_json()
        observed_peak = max(observed_peak, snapshot["staging_now"])
        samples += 1
        if snapshot["state"] != "running":
            return snapshot, observed_peak
        time.sleep(0.02)
    raise RuntimeError("job did not finish in time")


def scenario_slow_block(client) -> dict:
    print("\n== 场景 A：块 3 拖慢 700ms（后块先压好，写入仍按原序） ==")
    created = client.post(
        "/api/jobs",
        json={
            "level": 6,
            "base_delay_ms": 80,
            "slow_blocks": {"3": 700},
        },
    )
    if created.status_code != 201:
        check("作业创建", False, json.dumps(created.get_json(), ensure_ascii=False))
        return {}
    snap, observed_peak = wait(client, created.get_json()["job_id"])
    print("  compressed_order:", snap["compressed_order"])
    print("  written_order   :", snap["written_order"])
    print(
        "  staging_peak(server):",
        snap["staging_peak"],
        "| staging_now peak(polled):",
        observed_peak,
    )

    check("作业完成", snap["state"] == "complete", str(snap.get("error")))
    check(
        "写入严格按原块序 0–15",
        snap["written_order"] == list(range(BLOCK_COUNT)),
        f"written_count={snap['written_count']}",
    )
    rank3 = snap["compressed_rank"].get("3")
    rank4 = snap["compressed_rank"].get("4")
    check(
        "后块 4 先于慢块 3 压好",
        rank4 < rank3,
        f"压完排名：块4=#{rank4} 块3=#{rank3}",
    )
    check(
        "慢块 3 虽然后压好，但仍先于块 4 写出",
        snap["written_order"].index(3) < snap["written_order"].index(4),
        f"写序排名：块3=#{snap['written_rank']['3']} 块4=#{snap['written_rank']['4']}",
    )
    check(
        "暂存峰值 ≤ 2（含线程内未交出的块）",
        snap["staging_peak"] <= MAX_STAGED and observed_peak <= MAX_STAGED,
        f"server={snap['staging_peak']} polled={observed_peak}",
    )
    check(
        "每次写入时的暂存数 ≤ 2",
        all(e["staged_at_write"] <= MAX_STAGED for e in snap["write_events"]),
    )

    download = client.get(f"/api/jobs/{snap['job_id']}/download")
    check("下载返回 200", download.status_code == 200, f"HTTP {download.status_code}")
    data = gzip.decompress(download.data)
    sha = hashlib.sha256(data).hexdigest()
    check("解压后与内置样例逐字节一致", data == SAMPLE, f"sha256={sha[:16]}…")
    check("解压后 sha256 与样例一致", sha == SAMPLE_SHA)
    check(
        "gzip 尾部 ISIZE 等于原样例长度",
        int.from_bytes(download.data[-4:], "little") == len(SAMPLE),
    )
    check(
        "gzip 尾部 CRC32 等于原样例 CRC32",
        int.from_bytes(download.data[-8:-4], "little")
        == zlib.crc32(SAMPLE) & 0xFFFFFFFF,
    )
    check("下载响应带 Content-Length", "Content-Length" in download.headers)
    return snap


def scenario_write_failure(client) -> dict:
    print("\n== 场景 B：第 5 次块写入时模拟写盘失败 ==")
    created = client.post(
        "/api/jobs",
        json={
            "level": 6,
            "base_delay_ms": 60,
            "slow_blocks": {"2": 400},
            "fail_write_at": 5,
        },
    )
    if created.status_code != 201:
        check("作业创建", False, json.dumps(created.get_json(), ensure_ascii=False))
        return {}
    job_id = created.get_json()["job_id"]
    snap, observed_peak = wait(client, job_id)
    print("  state:", snap["state"])
    print("  error:", snap["error"])
    print("  written_order:", snap["written_order"])

    check("作业状态为 failed", snap["state"] == "failed")
    check("错误信息包含写盘失败原因", "写盘失败" in (snap["error"] or ""), snap.get("error") or "")
    check("只写出了失败点之前的块", snap["written_order"] == [0, 1, 2, 3])
    check("页面不可下载", snap["download_ready"] is False and snap["download_url"] is None)
    check("暂存已释放", snap["staging_now"] == 0, f"staging_now={snap['staging_now']}")

    download = client.get(f"/api/jobs/{job_id}/download")
    check("下载接口拒绝残包", download.status_code == 409, f"HTTP {download.status_code}")

    job_dir = app_module.manager.get(job_id).job_dir
    leftovers = sorted(p.name for p in job_dir.iterdir()) if job_dir.exists() else []
    check("产物目录无残留文件", leftovers == [], str(leftovers))
    check("暂存峰值未超限", snap["staging_peak"] <= MAX_STAGED,
          f"peak={snap['staging_peak']} polled={observed_peak}")
    return snap


def scenario_mkdir_failure_retry(client) -> None:
    print("\n== 场景 C：作业目录创建失败（如卷只读/权限不足），随后安全重试 ==")
    manager = app_module.manager
    jobs_before = dict(manager._jobs)
    real_mkdir = pathlib.Path.mkdir

    def boom(self, *args, **kwargs):
        if "jobs" in self.parts:
            raise OSError("模拟 EACCES：建目录失败")
        return real_mkdir(self, *args, **kwargs)

    pathlib.Path.mkdir = boom
    try:
        failed = client.post("/api/jobs", json={"level": 6})
    finally:
        pathlib.Path.mkdir = real_mkdir
    print("  首次开单 HTTP:", failed.status_code, failed.get_json())
    check("建目录失败返回 500", failed.status_code == 500)
    check("错误信息说明建目录失败", "创建作业目录失败" in failed.get_json()["error"])

    # The failed attempt must not add any registry entry (earlier scenarios
    # may already have jobs, so compare against the snapshot taken before).
    check("失败的一单未注册、不占最近作业", manager._jobs == jobs_before
          and (not jobs_before or manager.latest().id == next(reversed(jobs_before))))
    jobs_root = Path(manager.data_dir) / "jobs"
    new_dirs = [p for p in jobs_root.iterdir() if p.name not in jobs_before] \
        if jobs_root.exists() else []
    check("失败尝试未留下任何作业目录", new_dirs == [], str([p.name for p in new_dirs]))

    retried = client.post("/api/jobs", json={"level": 6})
    check("故障恢复后重试直接成功", retried.status_code == 201, str(retried.get_json()))
    snap = wait(client, retried.get_json()["job_id"])[0]
    check("重试作业正常完成", snap["state"] == "complete", str(snap.get("error")))
    download = client.get(f"/api/jobs/{snap['job_id']}/download")
    check("重试产物可下载且解压一致",
          download.status_code == 200 and gzip.decompress(download.data) == SAMPLE)


def scenario_partial_thread_failure_retry(client) -> None:
    print("\n== 场景 D：仅部分压缩线程启动成功，回滚后安全重试 ==")
    manager = app_module.manager
    jobs_before = dict(manager._jobs)
    real_start = threading.Thread.start
    spawned: list[threading.Thread] = []

    def flaky_start(self):
        if self.name.startswith(f"compress-{COMPRESS_SLOTS - 1}-"):
            raise RuntimeError("模拟第二个压缩位启动失败")
        spawned.append(self)
        return real_start(self)

    threading.Thread.start = flaky_start
    try:
        failed = client.post("/api/jobs", json={"level": 6})
    finally:
        threading.Thread.start = real_start
    print("  部分启动开单 HTTP:", failed.status_code, failed.get_json())
    check("部分线程启动失败返回 500", failed.status_code == 500)
    check("错误信息说明线程启动失败", "作业线程启动失败" in failed.get_json()["error"])

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and any(t.is_alive() for t in spawned):
        time.sleep(0.05)
    check("已启动的线程全部退出", all(not t.is_alive() for t in spawned),
          str([(t.name, t.is_alive()) for t in spawned]))
    check("失败的一单未注册（不可查询）", manager._jobs == jobs_before)

    jobs_root = Path(manager.data_dir) / "jobs"
    leftovers = [
        str(p.relative_to(jobs_root))
        for p in jobs_root.rglob("*")
        if p.relative_to(jobs_root).parts[0] not in jobs_before
    ] if jobs_root.exists() else []
    check("本次临时文件与成品全部清掉", leftovers == [], str(leftovers))

    retried = client.post("/api/jobs", json={"level": 6})
    check("回滚后重试直接成功", retried.status_code == 201, str(retried.get_json()))
    snap = wait(client, retried.get_json()["job_id"])[0]
    check("重试作业完整起齐 3 个线程并完成",
          snap["state"] == "complete"
          and len(manager.get(snap["job_id"]).threads) == COMPRESS_SLOTS + 1)


def scenario_conflict_during_startup(client) -> None:
    print("\n== 场景 E：启动窗口内另一单 409，旧备份仍可查可下 ==")
    old = client.post("/api/jobs", json={"level": 6})
    old_id = old.get_json()["job_id"]
    assert wait(client, old_id)[0]["state"] == "complete"

    gate_held = threading.Event()
    release = threading.Event()
    real_start = compressor.Job.start

    def slow_start(self):
        gate_held.set()
        assert release.wait(10)
        return real_start(self)

    compressor.Job.start = slow_start
    holder_error: list[Exception] = []

    def hold_gate():
        try:
            app_module.manager.create(JobSettings(level=6))
        except Exception as exc:  # pragma: no cover - surfaced via holder_error
            holder_error.append(exc)

    holder = threading.Thread(target=hold_gate)
    holder.start()
    assert gate_held.wait(10)
    time.sleep(0.1)
    try:
        conflict = client.post("/api/jobs", json={"level": 6})
        check("启动/清理期间另一单返回 409", conflict.status_code == 409,
              str(conflict.get_json()))
        old_snap = client.get(f"/api/jobs/{old_id}").get_json()
        check("旧备份仍可查询", old_snap["state"] == "complete"
              and old_snap["download_ready"] is True)
        check("旧备份仍可下载",
              client.get(f"/api/jobs/{old_id}/download").status_code == 200)
    finally:
        release.set()
        holder.join(15)
        compressor.Job.start = real_start
    check("持闸的那一单最终成功", holder_error == [])
    new_job = app_module.manager.latest()
    new_job.wait(30)
    check("新作业注册并完成", new_job.snapshot()["state"] == "complete")


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="drill-verify-"))
    app_module.manager = app_module._prepare_data_dir(tmp)
    app_module.app.config.update(TESTING=True)

    with app_module.app.test_client() as client:
        meta = client.get("/api/meta").get_json()
        print("== 元信息 ==")
        print(
            f"  {meta['block_count']} 块 × {meta['block_size']} B = "
            f"{meta['total_size']} B，压缩位 {meta['compress_slots']}，"
            f"暂存上限 {meta['max_staged']}"
        )
        print("  样例 sha256:", meta["sample_sha256"])
        check("样例 sha256 与内置样例一致", meta["sample_sha256"] == SAMPLE_SHA)

        scenario_slow_block(client)
        scenario_write_failure(client)
        scenario_mkdir_failure_retry(client)
        scenario_partial_thread_failure_retry(client)
        scenario_conflict_during_startup(client)

    print("\n== 结果 ==")
    if failures:
        print(f"FAIL：{len(failures)} 项未通过：")
        for item in failures:
            print("  -", item)
        return 1
    print("全部检查通过。")
    print("临时数据目录：", tmp)
    return 0


if __name__ == "__main__":
    sys.exit(main())
