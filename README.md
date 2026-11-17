# 写入切流反向代理（FastAPI + SQLite）

值班工程师更新服务节点时，用本系统把写入流量从旧节点切到备用节点。核心保证：

- **不中断已送达的写入**：每个写入在投递前快照现役节点并持久化绑定，切流只改路由表，
  不取消在途请求；回执仍从**原送达节点**读取，连续多次切流也不打断它。
- **不重复投递**：代理对任何写入只发一次。连接失败标 `FAILED`、读超时标
  `TIMEOUT_UNCERTAIN`，都**不会**自动改投另一节点；后端两侧写入 ID 集合永不相交。
- **探测失败保持旧路由**：切流前先探目标节点 `/healthz`，失败返回 409，路由表不变。
- **超时显式不确定**：写入已在后端落库但回执超时（读超时）时返回 504 +
  `TIMEOUT_UNCERTAIN`，页面醒目提示，可事后点"向原节点查询"核实，由原节点就地更正状态。
- **排空判断依据全量持久写入**：未决 = `PENDING` + `TIMEOUT_UNCERTAIN`（SQL 全量聚合，
  不受"最近 200 条"展示限量影响），并叠加当前在途请求；只要旧节点还有未决写入，
  就显示 **DRAINING 排空中**，全部核实回执后才显示 **DRAINED 已排空，可安全下线**。
  代理重启不丢未决：排空结论从 SQLite 重建，崩溃重启后仍正确阻止下线。
- **快照与登记原子化**：路由快照、PENDING 落库、在途登记与切流互斥（同一把锁），
  并发写入 + 同时切流不会产生"已快照旧节点但排空统计看不到"的窗口。

## 目录结构

```
backend/            模拟写入后端（两个实例，各自 SQLite 保存收到的写入）
  app/main.py       慢回执 delay、健康故障注入 /control/fault、/healthz、/records
  Dockerfile
proxy/              FastAPI 反向代理 + 切流控制台
  app/main.py       代理、切流、探测、在途/排空跟踪、故障注入透传
  app/db.py         SQLite 持久层（nodes / route / writes / events）
  app/dashboard.html 切流网页（现役节点、排空状态、写入记录、事件日志）
  Dockerfile
docker-compose.yml  一键启动 proxy + backend-a + backend-b（含健康检查与数据卷）
scripts/selftest.py 纯标准库端到端自测（含排空判断专项场景）
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 控制台： http://localhost:8080/
```

- 后端各自把写入保存在卷 `backend-a-data` / `backend-b-data` 的 `/data/writes.db`；
  代理状态在卷 `proxy-data` 的 `/data/proxy.db`。
- 想演练回执超时，可把 compose 里 `WRITER_READ_TIMEOUT` 调小（如 `2`）后重建。

## 不用 Docker 的本地运行（开发/自测）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r proxy/requirements.txt        # 后端依赖相同
mkdir -p .run/data
NODE_NAME=A BACKEND_DB=$PWD/.run/data/a.db \
  python -m uvicorn app.main:app --app-dir backend --port 8001 &
NODE_NAME=B BACKEND_DB=$PWD/.run/data/b.db \
  python -m uvicorn app.main:app --app-dir backend --port 8002 &
NODE_A_NAME=A NODE_B_NAME=B \
BACKEND_A_URL=http://127.0.0.1:8001 BACKEND_B_URL=http://127.0.0.1:8002 \
PROXY_DB=$PWD/.run/data/proxy.db \
  python -m uvicorn app.main:app --app-dir proxy --port 8080
```

## 网页操作演练

1. 慢回执延迟填 `10` 秒，点"投递写入"，在请求返回前立刻点另一节点的"切换流量"；
2. 观察：现役节点立即变更；旧节点卡片显示 **DRAINING 排空中** 与未决笔数；
   慢请求结束后仍由旧节点回执，写入只存在于旧节点；
3. 点某节点"注入健康故障"后再向它切流：收到 409 拦截提示，现役节点不变；
4. "拉取 A/B 的记录"可对比两个后端各自 SQLite 中保存的写入；
5. 超时写入显示红色 **TIMEOUT_UNCERTAIN**，点"向原节点查询"可核实真实落库情况；
6. 下线旧节点前：点该节点卡片上的"未决写入"，找全它名下**全部** PENDING /
   TIMEOUT_UNCERTAIN 写入（不限最近 200 条），逐笔或一键"全部逐笔核实"；
   全部确认回执后，卡片变为绿色 **DRAINED 已排空，可安全下线**。
   核实失败或节点暂不可达时写入保持未决，继续阻止排空。

## HTTP 接口（代理 8080）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/writes?delay=秒` | 投递写入（代理只投一次，绑定当前现役节点） |
| POST | `/api/switch` | body `{"target":"B"}`；探测失败 409 并保持旧路由 |
| GET  | `/api/status` | 现役节点、健康、全量排空/未决统计、最近 200 条写入与事件 |
| GET  | `/api/nodes/{name}/unresolved` | 该节点**全部**未决写入（PENDING+TIMEOUT_UNCERTAIN，无截断）与排空结论 |
| GET  | `/api/writes/{id}/refresh` | 向**原送达节点**查询回执并就地更新状态；失败/不可达保持未决 |
| GET  | `/api/nodes/{name}/records` | 透传该后端自己保存的写入记录 |
| POST | `/api/nodes/{name}/fault` | 注入/解除健康故障 `{"on": true|false}` |

## 自测

```bash
python -I scripts/selftest.py            # 自动拉起 3 个进程，跑完即清理
```

覆盖：普通写入无重复、慢回执在途切流、探测失败保旧路由、连续再次切流不打断在途写入、
读超时 504 不确定且不补投（事后向原节点核实可转 ACKED）、两侧后端记录不相交且与代理一一对应；
以及排空判断专项：TIMEOUT_UNCERTAIN/PENDING 阻止排空、早期未决被挤出最近 200 条展示列表仍不漏、
按节点找全未决并逐笔核实、代理崩溃重启后未决不丢、并发写入与切流交错后无错误排空结论。

## 关键环境变量

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `WRITER_READ_TIMEOUT` | 30 | 等回执读超时秒数，超时即结果不确定 |
| `CONNECT_TIMEOUT` | 3 | 连接超时，连接失败视为未送达 `FAILED` |
| `PROBE_TIMEOUT` | 2.5 | 切流健康探测超时 |
| `BACKEND_A_URL` / `BACKEND_B_URL` | 本地 8001/8002 | 两个后端地址 |
