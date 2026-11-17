#!/usr/bin/env python3
"""端到端自测（仅依赖标准库）：用临时目录拉起两个后端 + 一个代理并逐项核验。

覆盖：
  1. 普通写入落现役节点，备用节点无该写入（无重复投递）；
  2. 慢回执写入在途时切流：切流立即生效，新请求走新节点，
     在途写入仍由旧节点回执，且只存在于旧节点；
  3. 目标节点健康故障时切流被拦截（409），旧路由保持；
  4. 连续两次切流都不打断各自的在途写入（再次切流安全）；
  5. 回执超时：返回 504 + TIMEOUT_UNCERTAIN，不自动补投；
     事后向"原送达节点"查询可确认其实已落库并回执；
  6. 全量核对：两侧后端写入 ID 集合不相交，每条写入恰好出现一次；
  7. 排空判断（本系统修复重点）：
     - TIMEOUT_UNCERTAIN 与 PENDING 一样阻止排空（在途为 0 也不算排空）；
     - 排空依据全量持久写入：早期未决被挤出"最近 200 条"展示列表也不漏；
     - /api/nodes/{name}/unresolved 能按节点找全未决写入，逐笔核实后
       写入状态与排空结果同步更新；
     - 代理崩溃重启后持久化 PENDING 仍阻止排空，核实后才放行；
     - 并发写入与切流交错后无错误排空结论、无重复投递。

用法:
  python3 -I scripts/selftest.py [--python /path/to/venv/python]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASS, FAIL = 0, 0
RESULTS: list[tuple[bool, str]] = []


def check(cond: bool, name: str, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        RESULTS.append((True, f"PASS  {name}" + (f"  ({detail})" if detail else "")))
        print(f"  \033[32mPASS\033[0m  {name} {detail}")
    else:
        FAIL += 1
        RESULTS.append((False, f"FAIL  {name}  {detail}"))
        print(f"  \033[31mFAIL\033[0m  {name} {detail}")


def http(method: str, url: str, body: dict | None = None, timeout: float = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_ready(url: str, timeout: float = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False


def wait_status(proxy: str, write_id: str, expect: str, timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, s = http("GET", f"{proxy}/api/status")
        for w in s["writes"]:
            if w["id"] == write_id:
                if w["status"] == expect:
                    return w
        time.sleep(0.15)
    raise AssertionError(f"write {write_id} never reached status {expect}")


def wait_refresh_acked(proxy: str, write_id: str, timeout: float = 20) -> dict:
    """轮询"向原节点核实"，直到该写入被确认 ACKED（等后端慢回执完成）。"""
    deadline = time.time() + timeout
    last: dict = {}
    while time.time() < deadline:
        _, ref = http("GET", f"{proxy}/api/writes/{write_id}/refresh")
        last = ref
        if ref.get("write", {}).get("status") == "ACKED":
            return ref
        time.sleep(0.3)
    raise AssertionError(f"write {write_id} never confirmed ACKED, last={last}")


class SlowPost(threading.Thread):
    def __init__(self, proxy: str, delay: float, payload: str):
        super().__init__(daemon=True)
        self.proxy, self.delay, self.payload = proxy, delay, payload
        self.result: tuple[int, dict] | None = None
        self.write_id: str | None = None
        self.error: Exception | None = None

    def run(self):
        try:
            self.result = http("POST", f"{self.proxy}/api/writes?delay={self.delay}",
                               {"payload": self.payload}, timeout=30)
            if self.result:
                self.write_id = self.result[1].get("id")
        except Exception as exc:  # 代理重启/连接被切时客户端会报错，记录下来即可
            self.error = exc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="cutover-selftest-"))
    logs = work / "logs"
    logs.mkdir()
    procs: list[subprocess.Popen] = []
    ports = {"proxy": 18000, "A": 18001, "B": 18002}
    proxy = f"http://127.0.0.1:{ports['proxy']}"

    def spawn(kind: str, app_dir: Path, env: dict, port: int) -> None:
        log = open(logs / f"{kind}.log", "wb")
        p = subprocess.Popen(
            [args.python, "-I", "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port), "--app-dir", str(app_dir)],
            env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT,
        )
        procs.append(p)

    try:
        print("== 启动两个后端 + 代理（读回执超时 2s，便于演练超时场景） ==")
        spawn("backend-a", ROOT / "backend",
              {"NODE_NAME": "A", "BACKEND_DB": str(work / "a.db")}, ports["A"])
        spawn("backend-b", ROOT / "backend",
              {"NODE_NAME": "B", "BACKEND_DB": str(work / "b.db")}, ports["B"])
        ok_a = wait_ready(f"http://127.0.0.1:{ports['A']}/healthz")
        ok_b = wait_ready(f"http://127.0.0.1:{ports['B']}/healthz")
        check(ok_a and ok_b, "两个后端启动并健康", f"A={ok_a} B={ok_b}")

        proxy_env = {
            "NODE_A_NAME": "A", "NODE_B_NAME": "B",
            "BACKEND_A_URL": f"http://127.0.0.1:{ports['A']}",
            "BACKEND_B_URL": f"http://127.0.0.1:{ports['B']}",
            "PROXY_DB": str(work / "proxy.db"),
            "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2", "PROBE_TIMEOUT": "2",
        }

        def spawn_proxy() -> subprocess.Popen:
            log = open(logs / "proxy.log", "ab")
            p = subprocess.Popen(
                [args.python, "-I", "-m", "uvicorn", "app.main:app",
                 "--host", "127.0.0.1", "--port", str(ports["proxy"]),
                 "--app-dir", str(ROOT / "proxy")],
                env={**os.environ, **proxy_env}, stdout=log, stderr=subprocess.STDOUT,
            )
            procs.append(p)
            return p

        proxy_proc = spawn_proxy()
        check(wait_ready(proxy + "/"), "代理启动并提供控制台页面")

        _, st = http("GET", f"{proxy}/api/status")
        check(st["active"] == "A", "初始现役节点为 A", f"active={st['active']}")

        print("\n== 场景 1：普通写入只落现役节点 A ==")
        code, w0 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "base-write"})
        check(code == 201 and w0["status"] == "ACKED", "普通写入成功回执",
              f"id={w0.get('id','')[:8]} node={w0.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        check(any(r["id"] == w0["id"] for r in rec_a["records"]), "写入存在于 A 的 SQLite")
        check(not any(r["id"] == w0["id"] for r in rec_b["records"]), "B 上没有该写入（未重复投递）")

        print("\n== 场景 2：慢回执在途切流 A -> B，不中断、不重复 ==")
        slow1 = SlowPost(proxy, 1.2, "slow-during-cutover-1")
        slow1.start()
        # 等代理把 PENDING 行落库（请求已送达 A、回执未归）
        deadline = time.time() + 5
        pending_id = None
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for row in s["writes"]:
                if row["status"] == "PENDING":
                    pending_id = row["id"]
                    inflight = s["drainage"]["A"]["inflight_now"]
                    check(inflight >= 1, "切流前 A 显示在途写入", f"inflight_now={inflight}")
                    break
            if pending_id:
                break
            time.sleep(0.1)
        check(bool(pending_id), "慢写入已进入 PENDING（已送达、待回执）")

        code, sw = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200 and sw["ok"] and sw["active"] == "B", "在途切流 A -> B 成功")
        _, s2 = http("GET", f"{proxy}/api/status")
        check(s2["drainage"]["A"]["draining"] is True, "旧节点 A 进入排空调（DRAINING）")

        slow1.join(timeout=10)
        check(not slow1.is_alive() and slow1.result and slow1.result[0] == 201,
              "在途写入未被切流打断，仍正常拿到回执")
        check(slow1.write_id == pending_id and slow1.result[1]["node"] == "A",
              "回执仍来自原送达节点 A", f"node={slow1.result[1].get('node') if slow1.result else '?'}")
        row = wait_status(proxy, pending_id, "ACKED")
        check(row["target_node"] == "A", "代理记录中该写入始终绑定 A")

        code, w_new = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "after-switch"})
        check(code == 201 and w_new["node"] == "B", "切流后新请求立即走新节点 B",
              f"node={w_new.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(pending_id in ids_a and pending_id not in ids_b, "慢写入只在 A，未被投到 B")
        check(w_new["id"] in ids_b and w_new["id"] not in ids_a, "新写入只在 B")

        print("\n== 场景 3：目标节点探测失败 -> 保持旧路由 ==")
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": True})
        code, blocked = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 409 and blocked.get("blocked") and blocked.get("active") == "B",
              "A 故障时切向 A 被 409 拦截，现役仍为 B", blocked.get("reason", "")[:90])
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": False})
        code, sw2 = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw2["active"] == "A", "故障解除后切流成功，现役回到 A")

        print("\n== 场景 4：连续再次切流，各自在途写入都绑定原节点 ==")
        t1 = SlowPost(proxy, 1.2, "roundtrip-a")
        t1.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200, "第二次切流 A -> B 成功且不打断在途写入")
        t1.join(timeout=10)
        check(t1.result and t1.result[1]["node"] == "A", "切走前送达 A 的写入仍由 A 回执")

        t2 = SlowPost(proxy, 1.2, "roundtrip-b")
        t2.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200, "第三次切流 B -> A 成功")
        t2.join(timeout=10)
        check(t2.result and t2.result[1]["node"] == "B",
              "切走前送达 B 的写入仍由 B 回执（再次切流不打断它）")

        print("\n== 场景 5：回执超时 -> 结果不确定，不补投，可事后向原节点核实 ==")
        active_before = http("GET", f"{proxy}/api/status")[1]["active"]
        t0 = time.time()
        code, tout = http("POST", f"{proxy}/api/writes?delay=5",
                          {"payload": "timeout-case"}, timeout=15)
        elapsed = time.time() - t0
        check(code == 504 and tout.get("uncertain") and
              tout["status"] == "TIMEOUT_UNCERTAIN",
              f"读超时返回 504 + TIMEOUT_UNCERTAIN（{elapsed:.1f}s 触发）")
        timeout_id = tout["id"]
        timeout_node = tout["node"]
        _, s3 = http("GET", f"{proxy}/api/status")
        row = next(w for w in s3["writes"] if w["id"] == timeout_id)
        check(row["status"] == "TIMEOUT_UNCERTAIN", "超时状态已落库，页面可见结果不确定")
        _, rec_other = http("GET", f"{proxy}/api/nodes/"
                            f"{'B' if timeout_node == 'A' else 'A'}/records")
        check(not any(r["id"] == timeout_id for r in rec_other["records"]),
              "超时后代理没有向另一节点补投")

        print("   等待后端慢回执结束后，向原送达节点查询核实……")
        time.sleep(4.2)
        code, ref = http("GET", f"{proxy}/api/writes/{timeout_id}/refresh")
        check(code == 200 and ref["write"]["status"] == "ACKED"
              and ref["write"]["target_node"] == timeout_node,
              "向原节点查询后确认：写入其实已落库并回执，状态就地更正，未换节点")

        print("\n== 场景 6：全量核对，两侧后端写入不相交、每条恰好一次 ==")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(ids_a.isdisjoint(ids_b), "A/B 后端写入 ID 集合不相交（无任何重复投递）")
        _, s_all = http("GET", f"{proxy}/api/status")
        proxy_ids = {w["id"] for w in s_all["writes"]}
        check(proxy_ids == ids_a | ids_b,
              f"代理侧 {len(proxy_ids)} 条写入与后端记录一一对应",
              f"A={len(ids_a)} B={len(ids_b)}")

        # 直接打开后端 SQLite 核对持久化（后端各自保存写入的硬性要求）
        for node, db_path in (("A", work / "a.db"), ("B", work / "b.db")):
            con = sqlite3.connect(db_path)
            n = con.execute("SELECT COUNT(*) FROM writes").fetchone()[0]
            con.close()
            check(n == len(ids_a if node == "A" else ids_b),
                  f"后端 {node} 的 SQLite 持久化写入数 = {n}")

        print("\n== 场景 7：排空判断基于全量持久写入，未决（PENDING/超时）阻止排空 ==")
        print("-- 7.1 TIMEOUT_UNCERTAIN 即使在途为 0 也阻止排空 --")
        code, sw = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw["active"] == "A", "切流回 A，准备制造超时写入")
        code, tout2 = http("POST", f"{proxy}/api/writes?delay=5",
                           {"payload": "timeout-blocks-drain"}, timeout=15)
        check(code == 504 and tout2["status"] == "TIMEOUT_UNCERTAIN",
              "又一笔读超时写入落 A，状态 TIMEOUT_UNCERTAIN")
        timeout2 = tout2["id"]
        code, sw = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200 and sw["active"] == "B", "切流 A -> B，A 成为已切走的旧节点")
        _, s = http("GET", f"{proxy}/api/status")
        d_a = s["drainage"]["A"]
        check(d_a["inflight_now"] == 0 and d_a["timeout_uncertain"] == 1
              and d_a["unresolved"] == 1,
              "A 在途为 0 但全量统计看到 1 笔 TIMEOUT_UNCERTAIN 未决",
              f"inflight={d_a['inflight_now']} unresolved={d_a['unresolved']}")
        check(d_a["draining"] is True and d_a["drained"] is False,
              "TIMEOUT_UNCERTAIN 阻止排空：A 不会被误判为已排空")

        print("-- 7.2 早期未决被挤出最近 200 条展示列表时，排空判断仍正确 --")
        now = time.time()
        con = sqlite3.connect(work / "proxy.db")
        con.executemany(
            "INSERT INTO writes (id, target_node, payload, delay_hint, status, "
            "status_detail, created_at, updated_at, acked_at) "
            "VALUES (?, 'B', 'bulk', 0, 'ACKED', 'synthetic old acked', ?, ?, ?)",
            [(f"synthetic-{i:04d}", now + i * 0.001, now + i * 0.001,
              now + i * 0.001) for i in range(210)],
        )
        con.commit()
        con.close()
        _, s = http("GET", f"{proxy}/api/status")
        listed_ids = {w["id"] for w in s["writes"]}
        check(len(s["writes"]) == 200 and timeout2 not in listed_ids,
              "展示列表仍限量 200 条，早期 TIMEOUT_UNCERTAIN 已被挤出列表",
              f"listed={len(s['writes'])} total={s['writes_total']}")
        d_a = s["drainage"]["A"]
        check(d_a["timeout_uncertain"] == 1 and d_a["unresolved"] == 1
              and d_a["draining"] is True,
              "排空判断依据全量持久写入：被挤出列表的未决仍阻止排空")

        print("-- 7.3 按节点找全未决写入的接口 --")
        code, un_a = http("GET", f"{proxy}/api/nodes/A/unresolved")
        check(code == 200 and un_a["unresolved_count"] == 1
              and un_a["writes"][0]["id"] == timeout2 and un_a["draining"] is True,
              "GET /api/nodes/A/unresolved 找全被挤出的那笔未决写入")
        _, un_b = http("GET", f"{proxy}/api/nodes/B/unresolved")
        check(un_b["unresolved_count"] == 0, "B 没有未决写入")

        print("-- 7.4 核实后写入状态与排空结果同步更新 --")
        ref = wait_refresh_acked(proxy, timeout2)  # 轮询至后端 A 的 5s 慢回执完成
        check(ref["write"]["status"] == "ACKED",
              "向原节点 A 核实：超时写入确认已回执，状态转 ACKED")
        _, s = http("GET", f"{proxy}/api/status")
        check(s["drainage"]["A"]["draining"] is False
              and s["drainage"]["A"]["drained"] is True,
              "确认后排空结果同步更新：A 已排空，可安全下线")
        _, un_a = http("GET", f"{proxy}/api/nodes/A/unresolved")
        check(un_a["unresolved_count"] == 0, "A 的未决列表同步清空")

        print("-- 7.5 代理重启不丢未决：PENDING 仍阻止排空，核实后才放行 --")
        t_slow = SlowPost(proxy, 8.0, "pending-survives-restart")
        t_slow.start()
        pending2 = None
        deadline = time.time() + 5
        while time.time() < deadline:  # 等 PENDING 行落库（投递临界区之后即可见）
            _, un_b = http("GET", f"{proxy}/api/nodes/B/unresolved")
            if un_b["unresolved_count"] >= 1:
                pending2 = un_b["writes"][0]["id"]
                break
            time.sleep(0.1)
        check(bool(pending2), "B 上出现一笔在途 PENDING 写入")
        # SIGKILL 模拟崩溃式重启：在途请求被硬切断，PENDING 行留在 SQLite 里。
        proxy_proc.kill()
        proxy_proc.wait(timeout=10)
        time.sleep(0.3)
        proxy_proc = spawn_proxy()
        check(wait_ready(proxy + "/"), "代理带同一 SQLite 重启完成")
        _, s = http("GET", f"{proxy}/api/status")
        check(s["active"] == "B", "重启后现役节点从持久化路由恢复", s["active"])
        check(s["drainage"]["B"]["pending"] >= 1,
              "重启后在途内存态已清空，但持久化 PENDING 仍被统计",
              f"pending={s['drainage']['B']['pending']}")
        check(any("unresolved_writes_carried_over=1" in ev["message"]
                  for ev in s["events"] if ev["kind"] == "BOOT"),
              "重启事件日志记录结转未决写入数")
        code, sw = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw["active"] == "A", "重启后切流 B -> A")
        _, s = http("GET", f"{proxy}/api/status")
        check(s["drainage"]["B"]["draining"] is True,
              "重启+切流后：持久化 PENDING 阻止得出错误的'已排空'结论")
        t_slow.join(timeout=5)
        check(t_slow.error is not None or (t_slow.result and t_slow.result[0] >= 500),
              "被重启打断的客户端请求不会收到伪造回执")
        ref = wait_refresh_acked(proxy, pending2)  # 轮询至后端 B 的 8s 慢回执完成
        check(ref["write"]["status"] == "ACKED",
              "重启遗留的 PENDING 经向原节点 B 核实后转 ACKED")
        _, s = http("GET", f"{proxy}/api/status")
        check(s["drainage"]["B"]["drained"] is True, "核实后 B 才显示已排空")

        print("-- 7.6 并发写入与切流交错，最终无错误排空、无重复投递 --")
        threads = [SlowPost(proxy, 1.2, f"burst-{i}") for i in range(4)]
        for t in threads:
            t.start()
        time.sleep(0.4)  # 让 4 笔都进入在途
        code, sw = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200, "并发在途期间切流 A -> B 成功")
        fast = [http("POST", f"{proxy}/api/writes?delay=0",
                     {"payload": f"fast-{i}"}) for i in range(2)]
        check(all(c == 201 and w["node"] == "B" for c, w in fast),
              "切流后并发新写入都走 B")
        for t in threads:
            t.join(timeout=10)
        check(all(t.result and t.result[0] == 201 and t.result[1]["node"] == "A"
                  for t in threads),
              "4 笔并发在途写入都由原节点 A 回执，无一被打断或改投")
        _, s = http("GET", f"{proxy}/api/status")
        check(s["drainage"]["A"]["drained"] is True
              and s["drainage"]["A"]["unresolved"] == 0,
              "并发在途全部回执后，A 才真正排空")
        check(all(not s["drainage"][n]["draining"] for n in s["drainage"]),
              "全部节点无未决：没有任何错误的排空/未排空结论")

        print("-- 7.7 全量终核：代理 SQLite（剔除 synthetic）与两后端一一对应 --")
        con = sqlite3.connect(work / "proxy.db")
        rows = con.execute("SELECT id, status FROM writes").fetchall()
        con.close()
        real_ids = {r[0] for r in rows if not r[0].startswith("synthetic-")}
        check(sum(1 for r in rows if r[0].startswith("synthetic-")) == 210,
              "synthetic 旧记录全部保留在库")
        check(all(r[1] == "ACKED" for r in rows if not r[0].startswith("synthetic-")),
              "终态全部真实写入均为 ACKED（无残留未决）")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(ids_a.isdisjoint(ids_b), "交错与重启后两侧后端写入仍不相交")
        check(real_ids == ids_a | ids_b,
              "代理侧全部真实写入与后端记录一一对应",
              f"proxy={len(real_ids)} A={len(ids_a)} B={len(ids_b)}")

    finally:
        print("\n== 清理进程 ==")
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        if FAIL:
            print(f"\n日志保留在: {logs}")
        else:
            shutil.rmtree(work, ignore_errors=True)

    print(f"\n================ 自测结果: {PASS} 通过 / {FAIL} 失败 ================")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
