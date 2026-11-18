"""出队落库测试：钉死同败同成与争用/偏离口径。

红-绿节奏：
  先在朴素两步出队实现下运行，同败同成与争用用例必须红；
  再按出队规则簿选定的甲路径（删缓冲行与写正式同一事务）修复至全绿。
"""
import threading

import pytest
from fastapi.testclient import TestClient

import app as appmod
from app import app, connect, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    monkeypatch.setattr(appmod, "DEQUEUE_FAULT", False)
    init_db()
    yield


@pytest.fixture()
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def auth(client):
    tok = client.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return {"Authorization": f"Bearer {tok}"}


def make_buffer(batch_id: int | None = None, reason: str = "缺字段进双写缓冲") -> int:
    """直接造一条缓冲（兼容模式缺失字段也可经 validate 产生，这里取最短路径）。"""
    with connect() as c:
        if batch_id is None:
            cur = c.execute(
                "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
                " VALUES (?,?,?,?,?,?)",
                ("订单入站", "order_id,amount", "v3", "缓冲", "缺字段:currency", "2026-11-17T09:00:00"),
            )
            batch_id = cur.lastrowid
        cur = c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (batch_id, reason))
        return cur.lastrowid


def buffer_row(buf_id: int):
    with connect() as c:
        return c.execute("SELECT * FROM buffers WHERE id=?", (buf_id,)).fetchone()


def test_rule_book_pins_single_path_for_today(client, auth):
    """规则簿：当天选定一条路径后不许换，乙路径请求算偏离被拦下并记簿。"""
    r = client.get("/api/dequeue/rule-book", headers=auth)
    assert r.status_code == 200
    book = r.json()
    assert book["path"] == "甲"
    assert book["locked"] is True
    # 试图切换到乙路径 -> 409 出队偏了，且规则簿当天不变
    bad = client.post("/api/dequeue/select-path", headers=auth, json={"path": "乙"})
    assert bad.status_code == 409 and "偏" in bad.json()["detail"]
    again = client.get("/api/dequeue/rule-book", headers=auth).json()
    assert again["path"] == "甲"
    with connect() as c:
        dev = c.execute("SELECT COUNT(*) c FROM dequeue_ledger WHERE outcome='出队偏了'").fetchone()["c"]
    assert dev >= 1


def test_dequeue_atomic_success_and_alignment(client, auth):
    """正常出队：正式转通过与缓冲行删除同时生效，五处视图对齐。"""
    buf_id = make_buffer()
    r = client.post(f"/api/dequeue/{buf_id}", headers=auth)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["outcome"] == "出队成功"
    # 缓冲队列：行已删
    assert buffer_row(buf_id) is None
    # 正式列表：对应批次已是通过，且只有一条
    with connect() as c:
        batch = dict(c.execute("SELECT * FROM ingress_batches WHERE id=?", (body["batch_id"],)).fetchone())
        dup = c.execute(
            "SELECT COUNT(*) c FROM ingress_batches WHERE id=? AND result='通过'", (body["batch_id"],)
        ).fetchone()["c"]
    assert batch["result"] == "通过" and dup == 1
    # 详情与账册一致
    led = client.get("/api/dequeue/ledger", headers=auth).json()["items"]
    assert len(led) == 1 and led[0]["outcome"] == "出队成功" and led[0]["buffer_id"] == buf_id
    # 对账对齐
    recon = client.get("/api/reconcile", headers=auth).json()
    assert recon["aligned"] is True
    assert recon["dequeued"] == 1
    # 缓冲视图与正式视图不再同挂一条
    bufs = client.get("/api/buffers", headers=auth).json()["items"]
    assert all(b["id"] != buf_id for b in bufs)


def test_dequeue_half_success_rolls_back(client, auth, monkeypatch):
    """同败同成：写正式后、删缓冲前注入故障，整笔回滚，半成功必须红不允许残留。"""
    buf_id = make_buffer()
    monkeypatch.setattr(appmod, "DEQUEUE_FAULT", True)
    r = client.post(f"/api/dequeue/{buf_id}", headers=auth)
    assert r.status_code == 500
    monkeypatch.setattr(appmod, "DEQUEUE_FAULT", False)

    # 缓冲行必须仍在
    assert buffer_row(buf_id) is not None
    # 批次详情必须仍是「缓冲」，不得出现半成功的「通过」
    with connect() as c:
        row = c.execute(
            "SELECT i.* FROM ingress_batches i JOIN buffers b ON b.batch_id=i.id WHERE b.id=?",
            (buf_id,),
        ).fetchone()
    assert row["result"] == "缓冲"
    # 不得写成功账册
    with connect() as c:
        ok = c.execute("SELECT COUNT(*) c FROM dequeue_ledger WHERE outcome='出队成功'").fetchone()["c"]
    assert ok == 0
    # 故障后同一缓冲仍可正常出队（回滚干净，重试不被误伤）
    retry = client.post(f"/api/dequeue/{buf_id}", headers=auth)
    assert retry.status_code == 200 and retry.json()["outcome"] == "出队成功"
    assert buffer_row(buf_id) is None


def test_duplicate_dequeue_rejected(client, auth):
    """禁止出队后再出队、禁止正式重复挂同一缓冲。"""
    buf_id = make_buffer()
    first = client.post(f"/api/dequeue/{buf_id}", headers=auth)
    assert first.status_code == 200
    second = client.post(f"/api/dequeue/{buf_id}", headers=auth)
    assert second.status_code == 409
    assert "重复" in second.json()["detail"]
    with connect() as c:
        # 正式侧该批次只有一条通过记录
        n_pass = c.execute(
            "SELECT COUNT(*) c FROM ingress_batches WHERE id=? AND result='通过'",
            (first.json()["batch_id"],),
        ).fetchone()["c"]
        dup_log = c.execute(
            "SELECT COUNT(*) c FROM dequeue_ledger WHERE buffer_id=? AND outcome='重复出队拒绝'", (buf_id,)
        ).fetchone()["c"]
    assert n_pass == 1 and dup_log == 1


def test_concurrent_dequeue_only_one_wins(client, auth):
    """两名值班同时抢同一缓冲：排队执行，一个成功，后到只记出队争用。"""
    buf_id = make_buffer()
    results: list[tuple[str, int, dict]] = []
    barrier = threading.Barrier(2)

    def worker(tag: str):
        local = TestClient(app, raise_server_exceptions=False)
        tok = local.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
        h = {"Authorization": f"Bearer {tok}"}
        barrier.wait()
        resp = local.post(f"/api/dequeue/{buf_id}", headers=h)
        results.append((tag, resp.status_code, resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}))

    t1 = threading.Thread(target=worker, args=("甲值班",))
    t2 = threading.Thread(target=worker, args=("乙值班",))
    t1.start(); t2.start(); t1.join(); t2.join()

    statuses = sorted(code for _, code, _ in results)
    assert statuses == [200, 409], results
    loser = next(item for item in results if item[1] == 409)
    assert "争用" in loser[2].get("detail", "")
    with connect() as c:
        wins = c.execute("SELECT COUNT(*) c FROM dequeue_ledger WHERE buffer_id=? AND outcome='出队成功'", (buf_id,)).fetchone()["c"]
        contentions = c.execute("SELECT COUNT(*) c FROM dequeue_ledger WHERE buffer_id=? AND outcome='出队争用'", (buf_id,)).fetchone()["c"]
        buf_left = c.execute("SELECT COUNT(*) c FROM buffers WHERE id=?", (buf_id,)).fetchone()["c"]
    assert wins == 1 and contentions == 1 and buf_left == 0


def test_unknown_buffer_is_not_contention(client, auth):
    r = client.post("/api/dequeue/9999", headers=auth)
    assert r.status_code == 404
    with connect() as c:
        led = c.execute("SELECT COUNT(*) c FROM dequeue_ledger").fetchone()["c"]
    assert led == 0
