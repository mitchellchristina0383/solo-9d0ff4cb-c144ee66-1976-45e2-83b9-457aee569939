// UI smoke test for the "safe retry after a failed order" behaviour.
//
// Needs `npm i jsdom` and a running server whose POST /api/jobs fails at
// startup, e.g. a server started with the thread-start fault injection:
//
//   FAULT_PARTIAL_THREAD_START=1 gunicorn -w 1 --threads 16 \
//       --no-control-socket -b 127.0.0.1:8000 app:app
//   BASE_URL=http://127.0.0.1:8000 node tests/ui_retry_smoke.js
//
// Asserts the page shows the failure and keeps "开始压制备份" clickable so
// the same order can be retried once the fault is cleared.

const path = require("path");
const fs = require("fs");
const { JSDOM } = require("jsdom");

const ROOT = path.resolve(__dirname, "..");
const BASE = process.env.BASE_URL || "http://127.0.0.1:8000";
const html = fs.readFileSync(path.join(ROOT, "templates/index.html"), "utf8");
const js = fs.readFileSync(path.join(ROOT, "static/app.js"), "utf8");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const dom = new JSDOM(html, {
    url: BASE + "/",
    runScripts: "outside-only",
    pretendToBeVisual: true,
  });
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
  const start = doc.getElementById("start");
  const errBox = doc.getElementById("form-error");
  const submit = () =>
    doc
      .getElementById("job-form")
      .dispatchEvent(new window.Event("submit", { cancelable: true, bubbles: true }));

  submit();
  await sleep(800); // the failed POST settles quickly
  console.log("after failed create -> start.disabled:", start.disabled);
  console.log("error box:", errBox.textContent);
  const firstRetryable = start.disabled === false && errBox.textContent.includes("可重试");

  // A second failed attempt must not wedge the button either.
  submit();
  await sleep(800);
  console.log("after second failed create -> start.disabled:", start.disabled);
  const secondRetryable = start.disabled === false;

  const ok = firstRetryable && secondRetryable;
  console.log(ok ? "UI RETRY SMOKE PASS" : "UI RETRY SMOKE FAIL");
  process.exit(ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(2);
});
