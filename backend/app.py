
import hashlib, secrets, sqlite3, threading
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

# 测试用故障注入点：置 True 后，下一次出队在「写正式之后、删缓冲之前」抛错。
# 仅用于验证同败同成，生产代码路径不主动置位。
DEQUEUE_FAULT = False

# 出队争用判定（单机）。每个缓冲一个在途计数，请求一进入出队端点（认证后、
# 任何 IO/锁之前）即在 _REG_GUARD 内原子自增并取回序号：序号 0 为先到者，
# >0 表示已有同缓冲请求在途 -> 同时段后到争用者。登记是纯内存原子操作，
# 两次登记严格一先一后，序号不依赖后续线程调度；请求结束在 finally 递减。
_INFLIGHT: dict[int, int] = {}
_REG_GUARD = threading.Lock()


def _enter_dequeue(buffer_id: int) -> int:
    with _REG_GUARD:
        seq = _INFLIGHT.get(buffer_id, 0)
        _INFLIGHT[buffer_id] = seq + 1
        return seq


def _leave_dequeue(buffer_id: int) -> None:
    with _REG_GUARD:
        n = _INFLIGHT.get(buffer_id, 0)
        if n <= 1:
            _INFLIGHT.pop(buffer_id, None)
        else:
            _INFLIGHT[buffer_id] = n - 1


# 每个缓冲 id 一把专属锁：同一缓冲的写正式/删缓冲事务在此串行。
_BUFFER_LOCKS: dict[int, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


def _buffer_lock(buffer_id: int) -> threading.Lock:
    with _LOCKS_GUARD:
        lk = _BUFFER_LOCKS.get(buffer_id)
        if lk is None:
            lk = threading.Lock()
            _BUFFER_LOCKS[buffer_id] = lk
        return lk

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dequeue_rule_book(
          day TEXT PRIMARY KEY,
          path TEXT NOT NULL,
          locked INTEGER NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dequeue_ledger(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          day TEXT NOT NULL,
          buffer_id INTEGER,
          batch_id INTEGER,
          path TEXT NOT NULL,
          outcome TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        # 规则簿当天钉死甲路径：删缓冲行与写正式同一事务
        today = datetime.now().date().isoformat()
        if conn.execute("SELECT COUNT(*) c FROM dequeue_rule_book WHERE day=?", (today,)).fetchone()["c"] == 0:
            conn.execute(
                "INSERT INTO dequeue_rule_book(day,path,locked,created_at) VALUES (?,?,1,?)",
                (today, "甲", datetime.now().isoformat(timespec="seconds")),
            )
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))

@app.on_event("startup")
def _s():
    init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    with connect() as c:
        ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
        if not ctr:
            raise HTTPException(404, "无启用正式契约")
        need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
        missing = sorted(need - fields)
        now = datetime.now().isoformat(timespec="seconds")
        if missing:
            if ctr["mode"] == "兼容":
                cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                                (body.subject, body.payload_fields, ctr["version"], "缓冲", "缺字段:"+ ",".join(missing), now))
                c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (cur.lastrowid, "缺字段进双写缓冲"))
                return {"result":"缓冲", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
            cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                            (body.subject, body.payload_fields, ctr["version"], "拒收", "缺字段:"+ ",".join(missing), now))
            return {"result":"拒收", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
        cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now))
        return {"result":"通过", "version": ctr["version"], "batch_id": cur.lastrowid}

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
        buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
        dequeued = c.execute("SELECT COUNT(*) c FROM dequeue_ledger WHERE outcome='出队成功'").fetchone()["c"]
    return {
        "ingress_total": total,
        "passed": passed,
        "buffered": buffered,
        "rejected": rejected,
        "buffer_rows": buf_rows,
        "dequeued": dequeued,
        "aligned": buffered == buf_rows,
    }


def _today() -> str:
    return datetime.now().date().isoformat()


def _rule_book(conn) -> dict:
    row = conn.execute("SELECT * FROM dequeue_rule_book WHERE day=?", (_today(),)).fetchone()
    return dict(row) if row else {}


@app.get("/api/dequeue/rule-book")
def dequeue_rule_book(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        book = _rule_book(c)
    return {"day": book.get("day"), "path": book.get("path"), "locked": bool(book.get("locked", 0))}


class PathIn(BaseModel):
    path: str


def _record_ledger(conn, *, buffer_id, batch_id, path: str, outcome: str, detail: str) -> None:
    """账册留痕独立成笔，不与出队主事务同生共死，避免回滚吞掉争用/偏离记录。"""
    conn.execute(
        "INSERT INTO dequeue_ledger(day,buffer_id,batch_id,path,outcome,detail,created_at)"
        " VALUES (?,?,?,?,?,?,?)",
        (_today(), buffer_id, batch_id, path, outcome, detail,
         datetime.now().isoformat(timespec="seconds")),
    )


@app.post("/api/dequeue/select-path")
def select_dequeue_path(body: PathIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    conn = connect()
    try:
        book = _rule_book(conn)
        pinned = book.get("path")
        if body.path != pinned:
            # 留痕独立提交，不能被随后的 409 回滚吞掉
            _record_ledger(
                conn, buffer_id=None, batch_id=None, path=body.path,
                outcome="出队偏了",
                detail=f"当天已钉{pinned}路径，不得混用{body.path}出队支",
            )
            conn.commit()
            raise HTTPException(409, f"出队偏了：当天规则簿已钉{pinned}路径，不得混用出队支")
    finally:
        conn.close()
    return {"day": book.get("day"), "path": pinned, "locked": True}


@app.get("/api/dequeue/ledger")
def dequeue_ledger(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM dequeue_ledger ORDER BY id DESC").fetchall()
        return {"items": [dict(r) for r in rows]}


class _InjectedDequeueFault(RuntimeError):
    pass


@app.post("/api/dequeue/{buffer_id}")
def dequeue(buffer_id: int, authorization: str | None = Header(default=None)):
    """甲路径（规则簿当天钉死）：删缓冲行与写正式在同一事务，同败同成。

    争用身份：进入端点立即原子登记在途序号，序号 0 为先到者、>0 为同时段后到者；
    缓冲锁把同缓冲的事务串行，后到者在锁内复查缓冲行已被出队即只记「出队争用」。
    出队完成后的事后请求序号为 0、行已没，按「重复出队拒绝」处理，绝不重复挂正式。
    """
    require_user(authorization)
    # 任何 IO/锁之前先登记在途序号，纯内存原子操作，严格区分先到与后到
    seq = _enter_dequeue(buffer_id)
    is_contender = seq > 0
    conn = None
    try:
        conn = connect()
        # 规则簿当天路径（甲）；任何其它路径请求都在动数据前拦下
        book = _rule_book(conn)
        pinned = book.get("path") or "甲"
        if pinned != "甲":
            _record_ledger(
                conn, buffer_id=buffer_id, batch_id=None, path=pinned,
                outcome="出队偏了", detail="规则簿非甲路径，甲路径服务拒绝混用",
            )
            conn.commit()
            raise HTTPException(409, "出队偏了：出队落库只许按规则簿钉死的一条路径")

        lk = _buffer_lock(buffer_id)
        lk.acquire()  # 同一缓冲的事务串行；实际拿锁先后可能与在途序号先后不同
        try:
            # 主事务：立即拿 SQLite 写锁，写正式与删缓冲同生共死
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT batch_id FROM buffers WHERE id=?", (buffer_id,)).fetchone()
            if is_contender:
                # 序号 >0 即同时段后到争用者：无论拿锁先后、行此刻在不在，
                # 都不得写正式，只能等先到者落定后记一笔「出队争用」。
                conn.rollback()
                if row:
                    cid = row["batch_id"]  # 争用者反先拿锁：让出出队权，只记争用
                else:
                    won = conn.execute(
                        "SELECT batch_id FROM dequeue_ledger WHERE buffer_id=? AND outcome='出队成功' LIMIT 1",
                        (buffer_id,),
                    ).fetchone()
                    if not won:
                        raise HTTPException(404, "缓冲行不存在或已出队")
                    cid = won["batch_id"]
                _record_ledger(
                    conn, buffer_id=buffer_id, batch_id=cid, path=pinned,
                    outcome="出队争用", detail="同一缓冲行并发抢出队，后到仅记争用",
                )
                conn.commit()
                raise HTTPException(409, "出队争用：该缓冲已被先到的值班出队，后到仅记争用")
            if not row:
                # 先到者（序号 0）拿锁时行已没：只可能是更早一笔成功后的事后重复请求
                conn.rollback()
                won = conn.execute(
                    "SELECT batch_id FROM dequeue_ledger WHERE buffer_id=? AND outcome='出队成功' LIMIT 1",
                    (buffer_id,),
                ).fetchone()
                if not won:
                    # 无成功账册且锁内无行：从未属于本服务的缓冲 id
                    raise HTTPException(404, "缓冲行不存在或已出队")
                _record_ledger(
                    conn, buffer_id=buffer_id, batch_id=won["batch_id"], path=pinned,
                    outcome="重复出队拒绝", detail="缓冲已出队，禁止出队后再出队/正式重复挂同一缓冲",
                )
                conn.commit()
                raise HTTPException(409, "重复出队拒绝：该缓冲已出队，禁止重复挂正式")
            locked_batch_id = row["batch_id"]

            # 正式侧幂等闸门：同一缓冲批次不得重复挂正式
            formal = conn.execute(
                "SELECT result FROM ingress_batches WHERE id=?", (locked_batch_id,)
            ).fetchone()
            if not formal or formal["result"] == "通过":
                conn.rollback()
                _record_ledger(
                    conn, buffer_id=buffer_id, batch_id=locked_batch_id, path=pinned,
                    outcome="重复出队拒绝", detail="正式列表已挂该缓冲，禁止重复出队",
                )
                conn.commit()
                raise HTTPException(409, "重复出队拒绝：正式列表已挂同一缓冲，禁止出队后再出队")

            cur = conn.execute(
                "UPDATE ingress_batches SET result='通过', detail='出队转正式' WHERE id=? AND result!='通过'",
                (locked_batch_id,),
            )
            if cur.rowcount <= 0:  # 条件更新未命中，说明正式侧已挂 -> 按重复处理
                conn.rollback()
                _record_ledger(
                    conn, buffer_id=buffer_id, batch_id=locked_batch_id, path=pinned,
                    outcome="重复出队拒绝", detail="条件更新未命中，正式已存在",
                )
                conn.commit()
                raise HTTPException(409, "重复出队拒绝：正式列表已挂同一缓冲")
            if DEQUEUE_FAULT:
                # 测试注入点：故障必须触发整笔回滚，验证同败同成
                raise _InjectedDequeueFault("注入故障：正式已写、缓冲行尚未删除")
            conn.execute("DELETE FROM buffers WHERE id=?", (buffer_id,))
            conn.execute(
                "INSERT INTO dequeue_ledger(day,buffer_id,batch_id,path,outcome,detail,created_at)"
                " VALUES (?,?,?,?,'出队成功','甲路径单事务：写正式与删缓冲同败同成',?)",
                (_today(), buffer_id, locked_batch_id, pinned,
                 datetime.now().isoformat(timespec="seconds")),
            )
            conn.commit()
        finally:
            lk.release()
    except HTTPException:
        if conn is not None:
            conn.rollback()
        raise
    except Exception:
        # 任何故障：主事务整笔回滚，缓冲仍在、正式未挂，半成功被红
        if conn is not None:
            conn.rollback()
        raise HTTPException(500, "出队失败：写正式与删缓冲同事务回滚，未产生半成功")
    finally:
        if conn is not None:
            conn.close()
        _leave_dequeue(buffer_id)
    return {"outcome": "出队成功", "buffer_id": buffer_id, "batch_id": locked_batch_id}


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
