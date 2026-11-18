"""加强验证（独立进程外脚本，不走 TestClient）：
1) N 名值班并发抢同一缓冲，恰好 1 个成功，其余全部争用，正式只挂一次；
2) 反复故障注入，缓冲与正式始终保持出队前状态（零半成功）。
"""
import sys
import threading
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

import app as appmod  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

tmp = Path(tempfile.mkdtemp(prefix="dequeue_stress_"))
appmod.DB_PATH = tmp / "stress.db"
appmod.DEQUEUE_FAULT = False
appmod.init_db()


def login(client: TestClient) -> dict:
    tok = client.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return {"Authorization": f"Bearer {tok}"}


def make_buffer() -> int:
    with appmod.connect() as c:
        cur = c.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
            " VALUES (?,?,?,?,?,?)",
            ("订单入站", "order_id,amount", "v3", "缓冲", "缺字段:currency", "2026-11-17T10:00:00"),
        )
        bid = cur.lastrowid
        c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (bid, "缺字段进双写缓冲"))
        return c.execute("SELECT id FROM buffers WHERE batch_id=?", (bid,)).fetchone()["id"]


def stress_contention(n: int = 8) -> bool:
    buf_id = make_buffer()
    outcomes = []
    barrier = threading.Barrier(n)

    def worker():
        cli = TestClient(appmod.app, raise_server_exceptions=False)
        h = login(cli)
        barrier.wait()
        r = cli.post(f"/api/dequeue/{buf_id}", headers=h)
        outcomes.append(r.status_code)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok = outcomes.count(200)
    contented = outcomes.count(409)
    with appmod.connect() as c:
        buf_left = c.execute("SELECT COUNT(*) c FROM buffers WHERE id=?", (buf_id,)).fetchone()["c"]
        wins = c.execute(
            "SELECT COUNT(*) c FROM dequeue_ledger WHERE buffer_id=? AND outcome='出队成功'", (buf_id,)
        ).fetchone()["c"]
        contentions = c.execute(
            "SELECT COUNT(*) c FROM dequeue_ledger WHERE buffer_id=? AND outcome='出队争用'", (buf_id,)
        ).fetchone()["c"]
        passed = c.execute(
            "SELECT COUNT(*) c FROM ingress_batches WHERE id=(SELECT batch_id FROM dequeue_ledger WHERE buffer_id=? AND outcome='出队成功' LIMIT 1) AND result='通过'",
            (buf_id,),
        ).fetchone()["c"]
    good = ok == 1 and contented == n - 1 and buf_left == 0 and wins == 1 and contentions == n - 1 and passed == 1
    print(f"[争用压力 N={n}] 成功={ok} 争用={contented} 缓冲残留={buf_left} 成功账册={wins} 争用账册={contentions} 正式通过={passed} -> {'通过' if good else '失败'}")
    return good


def stress_fault(rounds: int = 10) -> bool:
    good = True
    for i in range(rounds):
        buf_id = make_buffer()
        appmod.DEQUEUE_FAULT = True
        cli = TestClient(appmod.app, raise_server_exceptions=False)
        h = login(cli)
        r = cli.post(f"/api/dequeue/{buf_id}", headers=h)
        appmod.DEQUEUE_FAULT = False
        with appmod.connect() as c:
            buf_left = c.execute("SELECT COUNT(*) c FROM buffers WHERE id=?", (buf_id,)).fetchone()["c"]
            still_buffer = c.execute(
                "SELECT COUNT(*) c FROM ingress_batches i JOIN buffers b ON b.batch_id=i.id WHERE b.id=? AND i.result='缓冲'",
                (buf_id,),
            ).fetchone()["c"]
            half = c.execute(
                "SELECT COUNT(*) c FROM ingress_batches i JOIN buffers b ON b.batch_id=i.id WHERE b.id=? AND i.result='通过'",
                (buf_id,),
            ).fetchone()["c"]
        ok = r.status_code == 500 and buf_left == 1 and still_buffer == 1 and half == 0
        good = good and ok
        if not ok:
            print(f"[故障轮 {i}] 异常: status={r.status_code} 缓冲残留={buf_left} 半成功={half}")
    print(f"[故障注入 {rounds} 轮] 零半成功、缓冲全保留 -> {'通过' if good else '失败'}")
    return good


if __name__ == "__main__":
    a = stress_contention()
    b = stress_fault()
    sys.exit(0 if a and b else 1)
