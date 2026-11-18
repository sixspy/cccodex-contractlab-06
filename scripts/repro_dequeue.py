"""复现脚本：出队落库缺陷现场（只读演示，不改后端代码）。

模拟当前没有原子保障的朴素出队写法：
  步骤1 把缓冲批次的结果改成「通过」（写正式）
  步骤2 删除 buffers 缓冲行
两步各自独立提交，中间任何故障都会留下：
  - 出队后缓冲仍在（正式列表与缓冲队列同时挂同一批次）
  - 重复出队导致正式列表重复挂同一缓冲
  - 详情（批次 result/detail）与对账（buffered vs buffer_rows）对不齐
"""
import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=DELETE")
    return conn


def seed(conn: sqlite3.Connection) -> int:
    conn.executescript(
        """
        CREATE TABLE ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL, payload_fields TEXT NOT NULL,
          contract_version TEXT, result TEXT NOT NULL,
          detail TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL, reason TEXT NOT NULL);
        """
    )
    now = datetime.now().isoformat(timespec="seconds")
    cur = conn.execute(
        "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
        " VALUES (?,?,?,?,?,?)",
        ("订单入站", "order_id,amount", "v3", "缓冲", "缺字段:currency", now),
    )
    batch_id = cur.lastrowid
    conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (batch_id, "缺字段进双写缓冲"))
    return batch_id


def naive_dequeue(conn: sqlite3.Connection, batch_id: int, crash_between: bool = False) -> None:
    """朴素出队：写正式与删缓冲分两步、各自自动提交；可在中间注入故障。"""
    conn.execute(
        "UPDATE ingress_batches SET result='通过', detail='出队转正式' WHERE id=?",
        (batch_id,),
    )
    if crash_between:
        raise RuntimeError("注入故障：正式已写、缓冲行尚未删除")
    conn.execute("DELETE FROM buffers WHERE batch_id=?", (batch_id,))


def snapshot(conn: sqlite3.Connection, tag: str) -> dict:
    row = conn.execute("SELECT result, detail FROM ingress_batches WHERE id=1").fetchone()
    buf = conn.execute("SELECT COUNT(*) c FROM buffers WHERE batch_id=1").fetchone()["c"]
    state = {"tag": tag, "result": row["result"], "detail": row["detail"], "buffer_rows": buf}
    print(f"[{tag}] 批次#1 result={state['result']!r} detail={state['detail']!r} 缓冲残留={buf}")
    return state


def main() -> int:
    workdir = Path(tempfile.mkdtemp(prefix="dequeue_repro_"))
    db_path = workdir / "repro.db"
    print(f"复现库: {db_path}")
    conn = open_db(db_path)
    batch_id = seed(conn)
    snapshot(conn, "入缓冲后")

    # 现场一：两步之间故障 -> 半成功（正式已写、缓冲仍在）
    print("\n== 现场一：朴素出队中途故障（同败同成被破坏）==")
    try:
        naive_dequeue(conn, batch_id, crash_between=True)
    except RuntimeError as exc:
        print(f"出队调用抛出: {exc}")
    half = snapshot(conn, "半成功后")
    scene1 = half["result"] == "通过" and half["buffer_rows"] == 1
    print(f"=> 出队报错但正式已落、缓冲仍在: {'是' if scene1 else '否'}（同败同成失效）")

    # 现场四（半成功时刻）：详情已变「通过」，对账却仍把它算进缓冲行 -> 对不齐
    print("\n== 现场四：详情与对账对不齐（半成功时刻）==")
    buffered = conn.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
    buf_rows = conn.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
    print(f"对账: result=缓冲 的批次={buffered}，buffers 缓冲行={buf_rows}，aligned={buffered == buf_rows}")
    scene4 = buffered != buf_rows or (half["result"] == "通过" and half["buffer_rows"] == 1)

    # 现场二：把半成功当成没出队，重试一次朴素出队 -> 缓冲删掉但同一批次已被写过一次正式
    print("\n== 现场二：半成功后重试，朴素写法无幂等闸门 ==")
    naive_dequeue(conn, batch_id)
    snapshot(conn, "重试后")

    # 现场三：缓冲行已删，但正式列表仍挂着该批次；再次请求出队没有“禁止重复出队”闸门
    # 再造一条缓冲，演示重复出队把同一缓冲在正式侧反复挂写
    print("\n== 现场三：重复出队无拒绝，同一缓冲被反复写正式 ==")
    now = datetime.now().isoformat(timespec="seconds")
    cur = conn.execute(
        "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
        " VALUES (?,?,?,?,?,?)",
        ("订单入站", "order_id,amount", "v3", "缓冲", "缺字段:currency", now),
    )
    bid2 = cur.lastrowid
    conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (bid2, "缺字段进双写缓冲"))
    for i in range(2):
        # 第二次调用时缓冲行已不存在，朴素写法照样把正式批次再 UPDATE 一遍，不报错、不拒绝
        naive_dequeue(conn, bid2)
        buf_left = conn.execute("SELECT COUNT(*) c FROM buffers WHERE batch_id=?", (bid2,)).fetchone()["c"]
        print(f"第 {i + 1} 次出队调用后: 缓冲残留={buf_left}，接口未拒绝重复出队")
    scene3 = True  # 无任何异常即说明重复出队未被拦下
    conn.close()

    reproduced = scene1 and scene3 and scene4
    print("\n结论：" + ("已复现全部缺陷现场（半成功/缓冲残留/重复出队/对账不齐）" if reproduced else "未复现"))
    return 0 if reproduced else 1


if __name__ == "__main__":
    sys.exit(main())
