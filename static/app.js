"use strict";

const POLL_MS = 250;
const STATE_LABEL = {
  running: "运行中",
  complete: "已完成",
  failed: "失败",
  cancelled: "已取消",
  idle: "空闲",
};

const state = {
  meta: null,
  job: null,
  slow: new Map(), // block index -> delay ms
  pollTimer: null,
  downloadUrl: null,
  sampleCache: null,
};

const $ = (id) => document.getElementById(id);

function fmtBytes(n) {
  if (n == null) return "—";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1024 / 1024).toFixed(2)} MiB`;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const isJson = (response.headers.get("Content-Type") || "").includes("application/json");
  const body = isJson ? await response.json() : null;
  if (!response.ok) {
    throw new Error((body && body.error) || `HTTP ${response.status}`);
  }
  return body;
}

// --- setup -----------------------------------------------------------------

async function loadMeta() {
  state.meta = await api("/api/meta");
  $("sample-sha").textContent = state.meta.sample_sha256;
  $("base-delay").max = String(state.meta.base_delay_max);
  buildBlockSelect();
  buildFailSelect();
  renderBlocks(null);
}

function buildBlockSelect() {
  const select = $("slow-block");
  select.innerHTML = "";
  for (let i = 0; i < state.meta.block_count; i += 1) {
    const option = document.createElement("option");
    option.value = String(i);
    option.textContent = `块 ${i}`;
    select.appendChild(option);
  }
}

function buildFailSelect() {
  const select = $("fail-at");
  select.innerHTML = '<option value="">不模拟</option>';
  for (let step = 1; step <= state.meta.block_count; step += 1) {
    const option = document.createElement("option");
    option.value = String(step);
    option.textContent = `第 ${step} 次块写入时失败`;
    select.appendChild(option);
  }
}

// --- slow-block chips -------------------------------------------------------

function renderSlowChips() {
  const list = $("slow-list");
  list.innerHTML = "";
  if (state.slow.size === 0) {
    list.innerHTML = '<span class="hint">尚未拖慢任何块。</span>';
    return;
  }
  for (const [index, delay] of [...state.slow.entries()].sort((a, b) => a[0] - b[0])) {
    const chip = document.createElement("span");
    chip.className = "chip";
    chip.append(`块 ${index} +${delay}ms `);
    const remove = document.createElement("button");
    remove.type = "button";
    remove.title = "移除";
    remove.setAttribute("aria-label", `移除块 ${index} 的拖慢`);
    remove.textContent = "×";
    chip.appendChild(remove);
    remove.addEventListener("click", () => {
      state.slow.delete(index);
      renderSlowChips();
    });
    list.appendChild(chip);
  }
}

function addSlowBlock() {
  const index = Number($("slow-block").value);
  const delay = Number($("slow-delay").value);
  $("form-error").textContent = "";
  if (!Number.isInteger(delay) || delay < 0 || delay > 10000) {
    $("form-error").textContent = "延迟需为 0–10000 毫秒的整数。";
    return;
  }
  if (delay === 0) {
    state.slow.delete(index);
    renderSlowChips();
    return;
  }
  if (state.slow.size >= 4 && !state.slow.has(index)) {
    $("form-error").textContent = "最多拖慢 4 个块——再多就说明不了“后块先压好”了。";
    return;
  }
  state.slow.set(index, delay);
  renderSlowChips();
}

// --- job lifecycle ----------------------------------------------------------

async function startJob(event) {
  event.preventDefault();
  $("form-error").textContent = "";
  const startButton = $("start");
  startButton.disabled = true;
  const payload = {
    level: Number($("level").value),
    base_delay_ms: Number($("base-delay").value) || 0,
    slow_blocks: Object.fromEntries(state.slow.entries()),
    fail_write_at: $("fail-at").value ? Number($("fail-at").value) : null,
  };
  try {
    const job = await api("/api/jobs", { method: "POST", body: JSON.stringify(payload) });
    state.job = job;
    state.downloadUrl = null;
    applyJob(job);
    startPolling(job.job_id);
  } catch (err) {
    // A 409/500 here means no job was created (or the attempt was rolled
    // back): nothing is running, so starting again must stay possible.
    $("form-error").textContent = `开单失败，可重试：${err.message}`;
    startButton.disabled = false;
    $("fail-now").disabled = true;
  }
}

async function failWriteNow() {
  if (!state.job || state.job.state !== "running") return;
  $("fail-now").disabled = true;
  $("form-error").textContent = "";
  try {
    const job = await api(`/api/jobs/${state.job.job_id}/fail-write`, { method: "POST" });
    applyJob(job);
  } catch (err) {
    $("form-error").textContent = `触发失败：${err.message}`;
    $("fail-now").disabled = false;
  }
}

async function cancelJob() {
  if (!state.job || state.job.state !== "running") return;
  $("cancel").disabled = true;
  try {
    const job = await api(`/api/jobs/${state.job.job_id}/cancel`, { method: "POST" });
    applyJob(job);
    stopPolling();
  } catch (err) {
    $("job-message").textContent = `取消失败：${err.message}`;
    $("job-message").className = "job-message error";
  }
}

function startPolling(jobId) {
  stopPolling();
  state.pollTimer = setInterval(async () => {
    try {
      const job = await api(`/api/jobs/${jobId}`);
      applyJob(job);
      if (job.state !== "running") stopPolling();
    } catch (err) {
      stopPolling();
      $("job-message").textContent = `轮询失败：${err.message}`;
      $("job-message").className = "job-message error";
    }
  }, POLL_MS);
}

function stopPolling() {
  if (state.pollTimer) {
    clearInterval(state.pollTimer);
    state.pollTimer = null;
  }
}

// --- rendering --------------------------------------------------------------

function applyJob(job) {
  if (!job) return;
  state.job = job;
  $("job-id").textContent = job.job_id;

  const badge = $("state-badge");
  badge.textContent = STATE_LABEL[job.state] || job.state;
  badge.className = `badge ${job.state}`;

  $("m-compressed").textContent = `${job.compressed_count} / ${job.block_count}`;
  $("m-written").textContent = `${job.written_count} / ${job.block_count}`;
  $("m-peak").textContent = String(job.staging_peak);
  $("m-staging").textContent = String(job.staging_now);
  $("m-inflight").textContent = String(job.inflight);
  $("m-inflight-peak").textContent = String(job.inflight_peak);
  $("m-elapsed").textContent = `${job.elapsed.toFixed(2)}s`;
  $("m-size").textContent = job.gz_size ? fmtBytes(job.gz_size) : "—";

  $("order-compressed").textContent = job.compressed_order.length
    ? job.compressed_order.join(" → ")
    : "—";
  $("order-written").textContent = job.written_order.length
    ? job.written_order.join(" → ")
    : "—";

  const message = $("job-message");
  message.className = "job-message";
  if (job.state === "running") {
    message.textContent = job.fail_next_armed
      ? "已布置写盘失败：下一次块写入时触发。"
      : "正在进行：蓝=压缩中，紫=已压好待写，绿=已写入。";
  } else if (job.state === "complete") {
    message.textContent = `完成：${job.written_count} 块按原序写入，暂存峰值 ${job.staging_peak}。`;
    message.classList.add("ok");
  } else if (job.state === "failed") {
    message.textContent = `作业失败：${job.error || "未知错误"}。残包已删除，暂存已释放。`;
    message.classList.add("error");
  } else if (job.state === "cancelled") {
    message.textContent = "作业已取消，残包已删除。";
  }

  $("cancel").disabled = job.state !== "running";
  $("start").disabled = job.state === "running";
  $("fail-now").disabled = job.state !== "running";

  const ready = Boolean(job.download_ready && job.download_url);
  const link = $("download-link");
  if (ready) {
    if (state.downloadUrl !== job.download_url) {
      state.downloadUrl = job.download_url;
      link.href = job.download_url;
      link.classList.remove("disabled");
      link.setAttribute("aria-disabled", "false");
      $("verify-result").textContent = "";
    }
    $("verify").disabled = false;
  } else {
    state.downloadUrl = null;
    link.href = "#";
    link.classList.add("disabled");
    link.setAttribute("aria-disabled", "true");
    $("verify").disabled = true;
    $("verify-result").textContent = "";
  }

  renderBlocks(job);
}

function renderBlocks(job) {
  const container = $("blocks");
  const count = state.meta ? state.meta.block_count : 16;
  const compressedRank = job ? job.compressed_rank || {} : {};
  const writtenRank = job ? job.written_rank || {} : {};

  if (container.children.length !== count) {
    container.innerHTML = "";
    for (let i = 0; i < count; i += 1) {
      const el = document.createElement("div");
      el.className = "block";
      el.dataset.index = String(i);
      el.dataset.status = "";
      el.innerHTML = `
        <span class="idx">${i}</span>
        <span class="meta"></span>
        <span class="rank">&nbsp;</span>
      `;
      container.appendChild(el);
    }
  }

  for (let i = 0; i < count; i += 1) {
    const info = job && job.blocks ? job.blocks[i] : null;
    const status = info ? info.status : "pending";
    const delay = info ? info.delay_ms : state.slow.get(i) || 0;
    const el = container.children[i];
    el.className = `block ${status}${delay > 0 ? " slow" : ""}`;
    el.title = buildBlockTitle(i, info, delay);
    el.querySelector(".meta").textContent = delay > 0
      ? `+${delay}ms`
      : fmtBytes(info ? info.size : null);

    const rank = el.querySelector(".rank");
    if (status === "written" && writtenRank[i]) {
      rank.textContent = `写 #${writtenRank[i]}`;
    } else if (status === "compressed" && compressedRank[i]) {
      rank.textContent = `压 #${compressedRank[i]}`;
    } else if (status === "compressing") {
      rank.textContent = info && info.slot != null ? `位 ${info.slot}` : "…";
    } else if (status === "dropped") {
      rank.textContent = "已丢弃";
    } else {
      rank.innerHTML = "&nbsp;";
    }
  }
}

function buildBlockTitle(index, info, delay) {
  if (!info) return `块 ${index}：待压缩`;
  const parts = [`块 ${index}`, `${fmtBytes(info.size)}`];
  if (delay > 0) parts.push(`拖慢 +${delay}ms`);
  parts.push(`状态 ${info.status}`);
  if (info.deflate_bytes) parts.push(`压缩后 ${fmtBytes(info.deflate_bytes)}`);
  return parts.join(" · ");
}

// --- browser-side verification ---------------------------------------------

async function verifyInBrowser() {
  if (!state.downloadUrl) return;
  const result = $("verify-result");
  const button = $("verify");
  button.disabled = true;
  result.textContent = "正在下载并解压…";
  try {
    if (!state.sampleCache) {
      const sampleResp = await fetch("/api/sample");
      if (!sampleResp.ok) throw new Error("无法下载原始样例");
      state.sampleCache = new Uint8Array(await sampleResp.arrayBuffer());
    }
    const gzResp = await fetch(state.downloadUrl);
    if (!gzResp.ok) throw new Error(`下载 gzip 失败：HTTP ${gzResp.status}`);
    const gzBytes = new Uint8Array(await gzResp.arrayBuffer());

    if (typeof DecompressionStream === "undefined") {
      result.textContent = "此浏览器不支持 DecompressionStream；请用下方命令本地校验。";
      return;
    }
    const stream = new Blob([gzBytes]).stream().pipeThrough(new DecompressionStream("gzip"));
    const out = new Uint8Array(await new Response(stream).arrayBuffer());
    const sample = state.sampleCache;
    const same = out.length === sample.length && out.every((byte, i) => byte === sample[i]);
    const digest = await crypto.subtle.digest("SHA-256", out);
    const hex = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
    if (same) {
      result.textContent = `一致 ✓ 解压后 ${fmtBytes(out.length)}，SHA-256 ${hex.slice(0, 16)}…`;
      result.className = "hint";
    } else {
      result.textContent = `不一致 ✗ 解压 ${out.length} 字节，SHA-256 ${hex.slice(0, 16)}…`;
      result.className = "error-text";
    }
  } catch (err) {
    result.textContent = `校验失败：${err.message}`;
    result.className = "error-text";
  } finally {
    button.disabled = false;
  }
}

// --- wiring -----------------------------------------------------------------

$("job-form").addEventListener("submit", startJob);
$("add-slow").addEventListener("click", addSlowBlock);
$("cancel").addEventListener("click", cancelJob);
$("fail-now").addEventListener("click", failWriteNow);
$("verify").addEventListener("click", verifyInBrowser);
$("download-link").addEventListener("click", (event) => {
  if (!state.downloadUrl) event.preventDefault();
});

renderSlowChips();
loadMeta().catch((err) => {
  $("form-error").textContent = `初始化失败：${err.message}`;
});
