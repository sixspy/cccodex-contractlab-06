"""端到端：对真实 uvicorn + 真实 SQLite 文件库走完整出队链路。"""
import json
import urllib.request

BASE = "http://127.0.0.1:10602"


def call(method: str, path: str, token: str | None = None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


status, body = call("POST", "/api/login", body={"username": "admin", "password": "123456"})
assert status == 200, body
token = body["token"]
print("登录:", body["username"], body["role"])

# 兼容模式契约：造一个缓冲（v4-shadow 是影子，不能用；直接用严格主题缺字段会拒收，
# 因此走最短路径：先验证拒收/通过主链路未受影响，再直接由校验缓冲需要兼容契约。
# v3 严格缺字段 -> 拒收；字段齐全 -> 通过（保证原行为不回归）
s, b = call("POST", "/api/ingress/validate", token, {"subject": "订单入站", "payload_fields": "order_id,amount"})
print("严格缺字段:", s, b["result"])
assert b["result"] == "拒收"
s, b = call("POST", "/api/ingress/validate", token, {"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
print("字段齐全:", s, b["result"])
assert b["result"] == "通过"

# 规则簿
s, book = call("GET", "/api/dequeue/rule-book", token)
print("规则簿:", book)
assert book == {"day": book["day"], "path": "甲", "locked": True}
s, b = call("POST", "/api/dequeue/select-path", token, {"path": "乙"})
print("切乙路径:", s, b)
assert s == 409 and "偏" in b["detail"]

# 直接造缓冲行（与测试夹具同法，走真实文件库）
import sqlite3
conn = sqlite3.connect("/workspace/data/app.db")
cur = conn.execute(
    "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
    " VALUES (?,?,?,?,?,?)",
    ("订单入站", "order_id,amount", "v3", "缓冲", "缺字段:currency", "2026-11-17T11:00:00"),
)
batch_id = cur.lastrowid
cur = conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (batch_id, "缺字段进双写缓冲"))
buf_id = cur.lastrowid
conn.commit()
conn.close()
print(f"造缓冲: buffer={buf_id} batch={batch_id}")

# 出队前对账
s, before = call("GET", "/api/reconcile", token)
print("出队前对账:", before)
assert before["aligned"] is True

# 正常出队
s, b = call("POST", f"/api/dequeue/{buf_id}", token)
print("出队:", s, b)
assert s == 200 and b["outcome"] == "出队成功" and b["batch_id"] == batch_id

# 五处视图
s, bufs = call("GET", "/api/buffers", token)
assert all(x["id"] != buf_id for x in bufs["items"]), "缓冲队列仍挂该缓冲"
s, batches = call("GET", "/api/ingress", token)
formal = [x for x in batches["items"] if x["id"] == batch_id]
assert len(formal) == 1 and formal[0]["result"] == "通过", formal
s, recon = call("GET", "/api/reconcile", token)
print("出队后对账:", recon)
assert recon["aligned"] is True and recon["dequeued"] == before.get("dequeued", 0) + 1
s, led = call("GET", "/api/dequeue/ledger", token)
print("账册首条:", led["items"][0])
assert led["items"][0]["outcome"] == "出队成功"

# 重复出队 -> 409 + 留痕，正式仍只有一条
s, b = call("POST", f"/api/dequeue/{buf_id}", token)
print("重复出队:", s, b)
assert s == 409 and "重复" in b["detail"]
s, led2 = call("GET", "/api/dequeue/ledger", token)
assert led2["items"][0]["outcome"] == "重复出队拒绝"

# 未知缓冲 -> 404 不留痕
n_led_before = len(led2["items"])
s, b = call("POST", "/api/dequeue/999999", token)
print("未知缓冲:", s, b)
assert s == 404
s, led3 = call("GET", "/api/dequeue/ledger", token)
assert len(led3["items"]) == n_led_before

# 前端页面与静态资源仍可访问（不另开页，沿用原入口）
with urllib.request.urlopen(BASE + "/") as r:
    html = r.read().decode()
assert "出队落正式" in html and "出队规则簿" in html and "出队账册" in html
with urllib.request.urlopen(BASE + "/static/vue.global.prod.js") as r:
    assert r.status == 200

print("\n端到端全部通过：五处视图对齐、同败同成、重复/偏离留痕、页面沿用原入口")
