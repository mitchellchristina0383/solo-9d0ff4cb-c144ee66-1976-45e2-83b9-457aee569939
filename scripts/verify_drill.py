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
import sys
import tempfile
import time
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as app_module  # noqa: E402
from compressor import (  # noqa: E402
    BLOCK_COUNT,
    MAX_STAGED,
    SAMPLE,
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
