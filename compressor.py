"""Ordered parallel gzip pipeline behind the Flask drill page.

The built-in backup sample is split into 16 fixed-size blocks.  Two
compression workers claim blocks in order and produce block-local deflate
fragments (``Z_SYNC_FLUSH`` terminated).  One writer drains finished fragments
strictly in original block order into a single gzip container: shared header,
concatenated deflate fragments, an empty final block and one CRC32/ISIZE
trailer.

Back-pressure
-------------
The admission window guarantees that at most ``MAX_STAGED`` (2) blocks are
claimed-but-unwritten at any time.  A claim is made *before* compressing, so a
block still sitting in a worker thread is already counted.  Claims are only
issued for the next two indices after ``next_write``; therefore the block whose
turn it is to be written can always make progress, no matter how many later
blocks are already staged.  This is the core invariant the page visualises.

Failure / cancellation
----------------------
The writer writes to ``backup.gz.part``.  On a simulated disk failure the job
is terminated, workers are stopped, staged fragments are dropped and both the
partial and final paths are removed.  Only a successfully finished job renames
the partial file to ``backup.gz`` and becomes downloadable.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import struct
import threading
import time
import uuid
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# --- sample / pipeline constants -------------------------------------------

BLOCK_COUNT = 16
BLOCK_SIZE = 48 * 1024
TOTAL_SIZE = BLOCK_COUNT * BLOCK_SIZE
COMPRESS_SLOTS = 2
MAX_STAGED = 2  # compressed-but-unwritten blocks, thread-held ones included

MIN_LEVEL = 1
MAX_LEVEL = 9
DEFAULT_LEVEL = 6
MAX_DELAY_MS = 10_000
MAX_BASE_DELAY_MS = 5_000


# --- the built-in backup sample --------------------------------------------


def _record(seq: int) -> bytes:
    op = "insert" if seq % 4 == 0 else "update"
    payload = f"{(seq * 2654435761) & 0xFFFFFFFF:08x}{seq:06d}"
    return (
        f'{{"seq":{seq:07d},"ts":"2026-11-26T12:{seq % 60:02d}:{(seq * 7) % 60:02d}Z",'
        f'"table":"orders","op":"{op}","user":"user-{seq % 211:03d}",'
        f'"payload":"{payload}"}}\n'
    ).encode()


def build_sample(total: int = TOTAL_SIZE) -> bytes:
    """Deterministic, lightly compressible backup sample (768 KiB)."""
    out = bytearray()
    seq = 0
    while len(out) < total:
        out += _record(seq)
        seq += 1
    return bytes(out[:total])


SAMPLE = build_sample()
assert len(SAMPLE) == TOTAL_SIZE, len(SAMPLE)
SAMPLE_SHA256 = hashlib.sha256(SAMPLE).hexdigest()


def split_blocks(data: bytes = SAMPLE) -> list[bytes]:
    return [data[i : i + BLOCK_SIZE] for i in range(0, len(data), BLOCK_SIZE)]


BLOCKS = split_blocks()


# --- errors -----------------------------------------------------------------


class JobError(Exception):
    """Expected, user-facing condition."""


class Conflict(JobError):
    """A job is already running."""


class NotFound(JobError):
    """Unknown job id."""


# --- gzip primitives --------------------------------------------------------


def deflate_block(data: bytes, level: int) -> bytes:
    """Deflate a block into a self-contained, sync-flushed fragment."""
    compressor = zlib.compressobj(level, zlib.DEFLATED, -15)
    return compressor.compress(data) + compressor.flush(zlib.Z_SYNC_FLUSH)


GZIP_HEADER = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff"


def gzip_trailer(crc: int, size: int) -> bytes:
    return struct.pack("<II", crc & 0xFFFFFFFF, size & 0xFFFFFFFF)


# --- job model --------------------------------------------------------------


@dataclass
class JobSettings:
    level: int = DEFAULT_LEVEL
    # Simulated per-block read/compress pacing so the timeline is observable.
    base_delay_ms: int = 0
    slow_blocks: dict[int, int] = field(default_factory=dict)
    fail_write_at: Optional[int] = None


@dataclass
class BlockState:
    index: int
    size: int
    delay_ms: int = 0
    slot: Optional[int] = None
    compress_started_at: Optional[float] = None
    compressed_at: Optional[float] = None
    written_at: Optional[float] = None
    dropped: bool = False
    deflate_bytes: int = 0

    @property
    def status(self) -> str:
        if self.written_at is not None:
            return "written"
        if self.dropped:
            return "dropped"
        if self.compressed_at is not None:
            return "compressed"
        if self.compress_started_at is not None:
            return "compressing"
        return "pending"


class Job:
    """One compression run: two workers plus an ordered writer."""

    def __init__(self, job_id: str, settings: JobSettings, data_dir: Path) -> None:
        self.id = job_id
        self.settings = settings
        self.data_dir = Path(data_dir)
        self.job_dir = self.data_dir / "jobs" / job_id
        self.partial_path = self.job_dir / "backup.gz.part"
        self.final_path = self.job_dir / "backup.gz"

        self.blocks = [
            BlockState(index=i, size=len(b)) for i, b in enumerate(BLOCKS)
        ]
        self.created_at = time.time()
        self.started_at = time.monotonic()
        self.finished_at: Optional[float] = None
        self.state = "running"  # running | complete | failed | cancelled
        self.error: Optional[str] = None
        self.crc32 = 0
        self.gz_size = 0

        self.next_claim = 0
        self.next_write = 0
        self.completed: dict[int, bytes] = {}
        self.compressed_order: list[int] = []
        self.written_order: list[int] = []
        self.in_write = False
        self.staging_peak = 0
        self.inflight_peak = 0
        # How often a worker wanted a block but the 2-block window was full.
        self.stall_events = 0
        # Page-triggered "make the very next block write fail".
        self.fail_next = False
        # (block index, staged count observed just before it was written)
        self.write_events: list[tuple[int, int]] = []

        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []

    # -- helpers --------------------------------------------------------

    def _unwritten_count(self) -> int:
        """Compressed (or being written) blocks not yet fully written."""
        return len(self.completed) + (1 if self.in_write else 0)

    def _drop_staged_locked(self) -> None:
        """Release every compressed-but-unwritten fragment.

        Covers fragments in the staging dict *and* the one the writer had just
        popped, so the page can show exactly which blocks were discarded.
        """
        for block in self.blocks:
            if block.compressed_at is not None and block.written_at is None:
                block.dropped = True
        self.completed.clear()

    def fail(self, message: str) -> None:
        with self.cond:
            if self.state != "running":
                return
            self.state = "failed"
            self.error = message
            self.finished_at = time.monotonic()
            self.stop_event.set()
            self._drop_staged_locked()
            self.cond.notify_all()

    def cancel(self) -> None:
        with self.cond:
            if self.state != "running":
                return
            self.state = "cancelled"
            self.error = "作业已被取消"
            self.finished_at = time.monotonic()
            self.stop_event.set()
            self._drop_staged_locked()
            self.cond.notify_all()

    def request_write_failure(self) -> bool:
        """Arm a simulated disk failure on the next block write.

        Returns False when the job is no longer running.
        """
        with self.cond:
            if self.state != "running":
                return False
            self.fail_next = True
            return True

    # -- snapshot --------------------------------------------------------

    def snapshot(self) -> dict:
        with self.cond:
            now = time.monotonic()
            elapsed = (self.finished_at or now) - self.started_at
            staged_now = self._unwritten_count()
            return {
                "job_id": self.id,
                "state": self.state,
                "finished": self.state != "running",
                "level": self.settings.level,
                "base_delay_ms": self.settings.base_delay_ms,
                "block_count": BLOCK_COUNT,
                "block_size": BLOCK_SIZE,
                "total_size": TOTAL_SIZE,
                "sample_sha256": SAMPLE_SHA256,
                "compress_slots": COMPRESS_SLOTS,
                "max_staged": MAX_STAGED,
                "slow_blocks": {
                    str(k): v for k, v in sorted(self.settings.slow_blocks.items())
                },
                "fail_write_at": self.settings.fail_write_at,
                "compressed_order": list(self.compressed_order),
                "written_order": list(self.written_order),
                "compressed_rank": {
                    str(i): rank + 1
                    for rank, i in enumerate(self.compressed_order)
                },
                "written_rank": {
                    str(i): rank + 1 for rank, i in enumerate(self.written_order)
                },
                "compressed_count": len(self.compressed_order),
                "written_count": len(self.written_order),
                "staging_now": staged_now,
                "staging_peak": self.staging_peak,
                # Claimed-but-unwritten: includes blocks still inside a
                # compression thread and the one the writer is writing now.
                "inflight": max(0, self.next_claim - self.next_write),
                "inflight_peak": self.inflight_peak,
                "stall_events": self.stall_events,
                "fail_next_armed": self.fail_next and self.state == "running",
                "write_events": [
                    {"block": idx, "staged_at_write": staged}
                    for idx, staged in self.write_events
                ],
                "error": self.error,
                "elapsed": round(elapsed, 3),
                "gz_size": self.gz_size,
                "crc32": f"{self.crc32:08x}" if self.crc32 else None,
                "download_ready": self.state == "complete"
                and self.final_path.is_file(),
                "download_url": f"/api/jobs/{self.id}/download"
                if self.state == "complete"
                else None,
                "blocks": [
                    {
                        "index": b.index,
                        "size": b.size,
                        "delay_ms": b.delay_ms,
                        "slot": b.slot,
                        "status": b.status,
                        "dropped": b.dropped,
                        "deflate_bytes": b.deflate_bytes,
                    }
                    for b in self.blocks
                ],
            }

    # -- lifecycle -------------------------------------------------------

    def start(self) -> None:
        self.job_dir.mkdir(parents=True, exist_ok=True)
        writer = threading.Thread(
            target=self._writer_main, name=f"writer-{self.id}", daemon=True
        )
        writer.start()
        self.threads.append(writer)
        for slot in range(COMPRESS_SLOTS):
            worker = threading.Thread(
                target=self._compress_main,
                args=(slot,),
                name=f"compress-{slot}-{self.id}",
                daemon=True,
            )
            worker.start()
            self.threads.append(worker)

    def wait(self, timeout: Optional[float] = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + timeout
        for thread in self.threads:
            remaining = (
                None if deadline is None else max(0.0, deadline - time.monotonic())
            )
            thread.join(remaining)
        return all(not thread.is_alive() for thread in self.threads)

    # -- compression workers --------------------------------------------

    def _claim_block(self, slot: int) -> Optional[int]:
        """Claim the next block, waiting for admission-window space."""
        stalled = False
        with self.cond:
            while not self.stop_event.is_set():
                window_full = (
                    self.next_claim < BLOCK_COUNT
                    and self.next_claim - self.next_write >= MAX_STAGED
                )
                if window_full:
                    if not stalled:
                        stalled = True
                        self.stall_events += 1
                    self.cond.wait(0.2)
                    continue
                if (
                    self.next_claim < BLOCK_COUNT
                    and self.next_claim - self.next_write < MAX_STAGED
                ):
                    index = self.next_claim
                    self.next_claim += 1
                    block = self.blocks[index]
                    block.slot = slot
                    block.compress_started_at = time.monotonic()
                    block.delay_ms = self.settings.slow_blocks.get(
                        index, 0
                    )
                    self.inflight_peak = max(
                        self.inflight_peak, self.next_claim - self.next_write
                    )
                    return index
                # All blocks claimed but not all written: wait for the writer.
                self.cond.wait(0.2)
            return None

    def _compress_main(self, slot: int) -> None:
        while True:
            index = self._claim_block(slot)
            if index is None:
                return
            delay_ms = self.settings.base_delay_ms + self.settings.slow_blocks.get(
                index, 0
            )
            if delay_ms > 0 and self.stop_event.wait(delay_ms / 1000.0):
                return
            try:
                fragment = deflate_block(BLOCKS[index], self.settings.level)
            except BaseException as exc:  # noqa: BLE001 - report and end job
                self.fail(f"块 {index} 压缩失败：{exc!r}")
                return
            with self.cond:
                if self.stop_event.is_set():
                    return  # aborted while compressing: drop the fragment
                self.completed[index] = fragment
                self.compressed_order.append(index)
                block = self.blocks[index]
                block.compressed_at = time.monotonic()
                block.deflate_bytes = len(fragment)
                self.staging_peak = max(self.staging_peak, self._unwritten_count())
                self.cond.notify_all()

    # -- ordered writer --------------------------------------------------

    def _writer_main(self) -> None:
        writer = ArchiveWriter(
            self.partial_path, self.final_path, fail_at=self.settings.fail_write_at
        )
        crc = 0
        total = 0
        try:
            writer.open()
            while True:
                with self.cond:
                    while (
                        self.next_write not in self.completed
                        and not self.stop_event.is_set()
                    ):
                        self.cond.wait(0.2)
                    if self.stop_event.is_set():
                        return
                    index = self.next_write
                    fragment = self.completed.pop(index)
                    self.in_write = True
                    staged_now = self._unwritten_count()
                    self.staging_peak = max(self.staging_peak, staged_now)
                    self.write_events.append((index, staged_now))
                    manual_failure = self.fail_next
                    if manual_failure:
                        self.fail_next = False
                # The in-turn block is written even if later blocks are staged.
                if manual_failure:
                    writer.steps += 1
                    raise OSError(
                        f"模拟 ENOSPC：手动触发，第 {writer.steps} 次块写入失败"
                    )
                writer.write_block(fragment)
                crc = zlib.crc32(BLOCKS[index], crc)
                total += len(BLOCKS[index])
                with self.cond:
                    self.in_write = False
                    if self.stop_event.is_set():
                        return  # aborted or cancelled mid-write; do not record
                    self.written_order.append(index)
                    self.blocks[index].written_at = time.monotonic()
                    self.next_write += 1
                    self.cond.notify_all()
                    last = self.next_write == BLOCK_COUNT
                if last:
                    break
            writer.close(crc, total)
            with self.cond:
                if self.state == "running":
                    self.crc32 = crc
                    self.gz_size = self.final_path.stat().st_size
                    self.state = "complete"
                self.finished_at = time.monotonic()
                self.stop_event.set()
                self.cond.notify_all()
        except OSError as exc:
            self.fail(f"写盘失败（第 {writer.steps} 次块写入）：{exc}")
        except BaseException as exc:  # noqa: BLE001 - never let a thread die silently
            self.fail(f"写盘线程异常：{exc!r}")
        finally:
            with self.cond:
                self.in_write = False
                if self.finished_at is None:
                    self.finished_at = time.monotonic()
                self.stop_event.set()
                self.cond.notify_all()
            if self.state != "complete":
                writer.abort()


class ArchiveWriter:
    """Ordered gzip writer with a simulated disk failure on step ``fail_at``."""

    def __init__(
        self, partial_path: Path, final_path: Path, fail_at: Optional[int] = None
    ) -> None:
        self.partial_path = Path(partial_path)
        self.final_path = Path(final_path)
        self.fail_at = fail_at
        self.steps = 0
        self._fh = None

    def open(self) -> None:
        self.partial_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.partial_path.open("wb")
        self._fh.write(GZIP_HEADER)
        self._fh.flush()

    def write_block(self, fragment: bytes) -> None:
        self.steps += 1
        if self.fail_at is not None and self.steps == self.fail_at:
            raise OSError(
                f"模拟 ENOSPC：写入第 {self.steps} 个块时磁盘写满"
            )
        if self._fh is None:
            raise OSError("写入器已关闭")
        self._fh.write(fragment)
        self._fh.flush()

    def close(self, crc: int, size: int) -> None:
        if self._fh is None:
            raise OSError("写入器已关闭")
        self._fh.write(b"\x03\x00")
        self._fh.write(gzip_trailer(crc, size))
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._fh = None
        os.replace(self.partial_path, self.final_path)

    def abort(self) -> None:
        if self._fh is not None:
            try:
                self._fh.close()
            except OSError:
                pass
            self._fh = None
        for path in (self.partial_path, self.final_path):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass


# --- job registry -----------------------------------------------------------


class JobManager:
    """In-memory registry; gunicorn must run a single process (threads)."""

    def __init__(self, data_dir: Path, keep: int = 8) -> None:
        self.data_dir = Path(data_dir)
        self.keep = keep
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()

    def create(self, settings: JobSettings) -> Job:
        with self._lock:
            running = [j for j in self._jobs.values() if j.state == "running"]
            if running:
                raise Conflict(f"已有作业 {running[0].id} 正在运行，请先等待或取消")
            job_id = uuid.uuid4().hex[:12]
            job = Job(job_id, settings, self.data_dir)
            self._jobs[job_id] = job
            self._prune_locked()
        job.start()
        return job

    def get(self, job_id: str) -> Job:
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            raise NotFound(f"未知作业 {job_id}")
        return job

    def latest(self) -> Optional[Job]:
        with self._lock:
            if not self._jobs:
                return None
            return next(reversed(self._jobs.values()))

    def _prune_locked(self) -> None:
        if len(self._jobs) <= self.keep:
            return
        done = [
            job_id
            for job_id, job in self._jobs.items()
            if job.state != "running"
            and job.finished_at is not None
        ]
        for job_id in done[: max(0, len(self._jobs) - self.keep)]:
            job = self._jobs.pop(job_id)
            shutil.rmtree(job.job_dir, ignore_errors=True)


def run_job(
    settings: JobSettings, data_dir: Path, job_id: Optional[str] = None
) -> Job:
    """Synchronous helper used by tests and the verification script."""
    job = Job(job_id or f"local-{uuid.uuid4().hex[:8]}", settings, Path(data_dir))
    job.start()
    job.wait(timeout=120)
    return job
