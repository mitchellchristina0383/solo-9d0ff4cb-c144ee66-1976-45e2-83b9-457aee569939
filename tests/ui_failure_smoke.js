// Optional failure-path UI smoke test (needs `npm i jsdom` and a running server).
//
//   BASE_URL=http://127.0.0.1:8000 node tests/ui_failure_smoke.js
//
// Starts a paced job, clicks "立刻写盘失败", and asserts the page shows an
// error, the download stays disabled and the endpoint refuses the partial.

const path = require("path");
const fs = require("fs");
const { JSDOM } = require("jsdom");

const ROOT = path.resolve(__dirname, "..");
const BASE = process.env.BASE_URL || "http://127.0.0.1:8000";
const html = fs.readFileSync(path.join(ROOT, "templates/index.html"), "utf8");
const js = fs.readFileSync(path.join(ROOT, "static/app.js"), "utf8");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const dom = new JSDOM(html, { url: BASE + "/", runScripts: "outside-only", pretendToBeVisual: true });
  const { window } = dom;
  window.fetch = (input, init) => {
    const url = String(input).startsWith("http") ? String(input) : BASE + input;
    return fetch(url, init);
  };
  const { webcrypto } = require("crypto");
  Object.defineProperty(window, "crypto", { value: webcrypto, configurable: true });
  window.DecompressionStream = DecompressionStream;
  window.Response = Response;
  window.Blob = Blob;
  window.eval(js);
  await sleep(600);

  const doc = window.document;
  doc.getElementById("base-delay").value = "50";
  doc.getElementById("slow-block").value = "5";
  doc.getElementById("slow-delay").value = "400";
  doc.getElementById("add-slow").click();
  doc.getElementById("job-form").dispatchEvent(new window.Event("submit", { cancelable: true, bubbles: true }));
  await sleep(500);
  const failButton = doc.getElementById("fail-now");
  console.log("live fail button present:", Boolean(failButton));
  failButton.click();

  let badge = "";
  for (let i = 0; i < 150; i += 1) {
    await sleep(100);
    badge = doc.getElementById("state-badge").textContent;
    if (badge !== "运行中") break;
  }

  const link = doc.getElementById("download-link");
  const message = doc.getElementById("job-message");
  const jobId = doc.getElementById("job-id").textContent;
  const resp = await fetch(`${BASE}/api/jobs/${jobId}/download`);
  const dropped = [...doc.querySelectorAll("#blocks .block.dropped")].map((el) => el.dataset.index);

  console.log("final badge:", badge);
  console.log("message:", message.textContent, "| class:", message.className);
  console.log("download href:", link.getAttribute("href"), "disabled?", link.classList.contains("disabled"));
  console.log("written:", doc.getElementById("m-written").textContent, "peak:", doc.getElementById("m-peak").textContent);
  console.log("dropped blocks shown:", dropped.join(",") || "(none)");
  console.log("download endpoint status:", resp.status);

  const ok =
    badge === "失败" &&
    message.className.includes("error") &&
    Boolean(failButton) &&
    link.classList.contains("disabled") &&
    !link.getAttribute("href").includes("/download") &&
    resp.status === 409;
  console.log(ok ? "FAILURE UI SMOKE PASS" : "FAILURE UI SMOKE FAIL");
  process.exit(ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(2);
});
