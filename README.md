# 乱序压缩 · 顺序落盘 —— gzip 并行压制备份演练台

一个 Flask 演练页：内置备份样例被切成 16 个 48 KiB 的块，交给 **两个并行压缩位**
压缩，再由 **一个写入器按原块序** 拼成 **单个 gzip 文件**。解压后必须与内置样例
逐字节一致。页面可以拖慢指定块（让后面的块先压好），也可以模拟写盘失败。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

端口可用 `DRILL_PORT=8080 docker compose up --build` 覆盖。

不用 Docker 时（Python 3.11）：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
gunicorn -w 1 --threads 16 --no-control-socket -b 127.0.0.1:8000 app:app
# 或开发模式：python app.py
```

> 必须单进程（`-w 1`）：作业状态在内存里，压缩位是 OS 线程。

## 核心机制

### 1. 并行压、顺序写、单成员 gzip

- 每块用 `zlib.compressobj(level, zlib.DEFLATED, -15)` 压成 **raw deflate 片段**，
  以 `Z_SYNC_FLUSH` 收尾（片段可独立衔接）。
- 写入器先写 10 字节 gzip 头，再按块号 0→15 依次追加片段，最后追加空的结束块
  `03 00` 和 **一个** CRC32/ISIZE 尾部。
- 因此产物是标准单成员 gzip：`gzip -dc`、`zlib`、浏览器 `DecompressionStream('gzip')`
  都能直接解，不需要 `Z_FULL_FLUSH` 之类的特殊处理。
- 解压校验两处：`/api/sample` 提供原始样例；页面「在浏览器里解压校验」按钮会拉取
  `backup.gz`、`DecompressionStream` 解压并对比 SHA-256；服务端/测试另有 `gzip.decompress`
  逐字节比对。

### 2. 暂存上限 2，且「在线程里的也算」

领块采用 **准入窗口** 而不是「先压完再看能不能放」：

```
next_claim - next_write < MAX_STAGED(=2)   # 才允许领取下一块
```

领块时就占掉一个名额，压缩线程里的块、`completed` 里待写的块、正在写的块都算名额。
所以「已压好但未写完」的块在任何瞬间 ≤ 2，峰值也被同一口径统计
（`staging_peak`，页面显示为「暂存峰值」）。

### 3. 暂存的后块堵不住轮到写入的块

写入器只等 `next_write` 这一块；后块先压好只会进 `completed` 字典，不改变写入顺序。
反过来，两个压缩位可能在等名额（写入慢时），这属于正常背压——**被拖慢的块只占住它
自己的压缩位，写入器始终能拿到它当前轮次需要的块**。页面的「在途（已领未写）」
指标和「压完顺序 / 写完顺序」两行把这一点直接摊开。

### 4. 写盘失败：立即终止、释放暂存、不留残包

- 写入器先写 `backup.gz.part`，全部成功后才 `os.replace` 成 `backup.gz`。
- 任一写盘错误（预设第 N 次块写入，或运行中点击「立刻写盘失败」）→ 作业 `failed`、
  停止两个压缩位、清空暂存（页面把这些块标为「已丢弃」）、删除 `.part` 和同名最终文件。
- 下载接口只对 `complete` 放行，其余一律 `409`；页面的下载按钮保持禁用。
- 取消作业走同一条清理路径。

### 5. 开单是原子操作：全部线程起来之前，这单不存在

- 开单 = 建作业目录 → 启动 1 个写入线程 + 2 个压缩线程。只有两步全部成功，作业才会被
  注册：此后才能按 id 查询、进入「最近作业」、参与旧包淘汰（保留最近 8 单）。
- **建目录失败**（卷只读/权限不足等）或**任一线程启动失败**（只起来部分线程）：
  接口返回 `500` 并说明原因；已启动的线程会被立刻叫停并等待退出，本次作业目录
  （含 `.part` 和任何成品）整个删掉，注册表不留任何痕迹——所以同一张页面可以直接重试，
  不会再撞「已有作业运行中」，也不可能产出可下载的残包。
- 启动或回滚清理期间再点「开始」立刻得到 `409`（另一作业正在启动或清理中）；这把开单闸
  与查询/下载路径分开，旧备份在整个窗口内始终可查询、可下载。


## 页面

| 区域 | 内容 |
| --- | --- |
| 控制 | 压缩级别、演示节奏（每块基础延迟）、拖慢指定块（最多 4 块）、预置/即时写盘失败 |
| 状态 | 压完块数、写完块数、暂存峰值（上限 2）、当前暂存、在途、耗时、gzip 大小、压完/写完顺序 |
| 块轨道 | 每块状态：待压 / 压缩中（显示压缩位）/ 已压好·暂存（显示压完名次）/ 已写入（显示写序名次）/ 已丢弃 |
| 下载 | 完成才可下载 `backup.gz`；支持浏览器内解压并比对 SHA-256 |

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/meta` | 块数、块大小、暂存上限、样例 SHA-256 等 |
| GET | `/api/sample` | 内置备份样例原文件 |
| POST | `/api/jobs` | 创建作业：`{"level":6,"base_delay_ms":120,"slow_blocks":{"3":700},"fail_write_at":5}` |
| GET | `/api/jobs/<id>` | 作业快照（含 `compressed_order`、`written_order`、`staging_peak`、`blocks`） |
| GET | `/api/jobs/latest` | 最近一个作业 |
| POST | `/api/jobs/<id>/fail-write` | 让下一次块写入失败（运行中可用） |
| POST | `/api/jobs/<id>/cancel` | 取消并清理 |
| GET | `/api/jobs/<id>/download` | 成功才返回 `application/gzip`，否则 409 |

同一时刻只允许一个作业（状态在内存中），重复创建返回 409。`POST /api/jobs` 的状态码：

- `201`：目录已建好且全部线程启动成功，作业开始运行。
- `400`：入参校验失败（没有任何线程被启动，可直接改参数重试）。
- `409`：已有作业运行中，或另一单正处于启动/回滚清理窗口。
- `500`：建目录或线程启动失败；本次尝试已完整回滚（线程退出、目录删净、未注册），
  排除故障后用同样的请求重试即可。

## 验证

```bash
pip install -r requirements-dev.txt
python -m pytest tests -q                 # 35 项：管线不变量 + Flask API + 原子开单回滚
python scripts/verify_drill.py            # 场景化演练：慢块、写盘失败、建目录/线程启动失败重试、开单 409
```

可选的前端冒烟测试（需要 `npm i` 装 jsdom，并在 `BASE_URL` 指向的服务器运行时）：

```bash
npm install
BASE_URL=http://127.0.0.1:8000 node tests/ui_smoke.js
BASE_URL=http://127.0.0.1:8000 node tests/ui_failure_smoke.js
# 开单失败重试：需要一个让 POST /api/jobs 启动失败的服务器（如注入线程启动故障）
BASE_URL=http://127.0.0.1:8000 node tests/ui_retry_smoke.js
```

## 目录

```
app.py               Flask 路由与入参校验
compressor.py        样例、Job（两个压缩位 + 顺序写入器）、JobManager
entrypoint.py        容器内修正数据卷属主后降权执行
templates/ static/   页面
tests/               pytest + 可选 jsdom 冒烟
scripts/verify_drill.py
```

## 已知限制

- 作业状态与暂存都在进程内存里：进程重启后进行中的作业丢失，旧作业产物也会在启动时清空
  （启动时清理 `data/jobs`，包括被杀进程留下的 `.part`）；仅适用于单进程演练，不是生产级备份器。
- 「拖慢块」是 `time.sleep` 模拟，不是真实 IO 慢；用于演示调度与背压。
- 样例固定 16 × 48 KiB（768 KiB），块数/块大小是常量（`compressor.py` 顶部）。
