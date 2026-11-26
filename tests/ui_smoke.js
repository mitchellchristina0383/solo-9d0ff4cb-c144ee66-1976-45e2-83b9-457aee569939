// Optional front-end smoke test (needs `npm i jsdom` and a running server).
//
//   BASE_URL=http://127.0.0.1:8000 node tests/ui_smoke.js
//
// Starts a job with block 3 slowed down, polls the DOM until it finishes and
// asserts the download becomes available and the in-browser gzip check passes.

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
  console.log("rendered blocks:", doc.querySelectorAll("#blocks .block").length);
  console.log("sample sha:", doc.getElementById("sample-sha").textContent);

  doc.getElementById("base-delay").value = "80";
  doc.getElementById("slow-block").value = "3";
  doc.getElementById("slow-delay").value = "700";
  doc.getElementById("add-slow").click();
  doc.getElementById("job-form").dispatchEvent(
    new window.Event("submit", { cancelable: true, bubbles: true }),
  );

  let badge = "";
  let peak = 0;
  let sawStaged = false;
  for (let i = 0; i < 150; i += 1) {
    await sleep(100);
    badge = doc.getElementById("state-badge").textContent;
    peak = Math.max(peak, Number(doc.getElementById("m-peak").textContent));
    if (doc.querySelectorAll("#blocks .block.compressed").length > 0) sawStaged = true;
    if (badge !== "运行中") break;
  }

  const link = doc.getElementById("download-link");
  const written = [...doc.querySelectorAll("#blocks .block.written")].map((el) =>
    Number(el.dataset.index),
  );
  console.log("final badge:", badge);
  console.log("compressed:", doc.getElementById("m-compressed").textContent);
  console.log("written:", doc.getElementById("m-written").textContent);
  console.log("peak:", doc.getElementById("m-peak").textContent, "saw staged block:", sawStaged);
  console.log("order:", doc.getElementById("order-compressed").textContent);
  console.log("download href:", link.getAttribute("href"));

  doc.getElementById("verify").click();
  await sleep(1500);
  const verify = doc.getElementById("verify-result").textContent;
  console.log("verify:", verify);

  const ok =
    badge === "已完成" &&
    peak <= 2 &&
    sawStaged &&
    written.length === 16 &&
    written.every((v, i) => v === i) &&
    link.getAttribute("href").includes("/download") &&
    /一致/.test(verify);
  console.log(ok ? "UI SMOKE PASS" : "UI SMOKE FAIL");
  process.exit(ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(2);
});
