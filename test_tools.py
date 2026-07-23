"""Verifica il Tool-Call Action Firewall (protezione delle AZIONI)."""
import threading, time, json
import httpx, uvicorn
from mock_llm_server import MockServerThread
import spend_proxy
from spend_proxy import tool_firewall

mock = MockServerThread(port=8931); mock.start(); mock.wait_until_ready()
cfg = uvicorn.Config(spend_proxy.app, host="127.0.0.1", port=8900, log_level="error")
server = uvicorn.Server(cfg); server.install_signal_handlers = lambda: None
threading.Thread(target=server.run, daemon=True).start()
while not getattr(server, "started", False): time.sleep(0.05)

BASE = "http://127.0.0.1:8900"
URL = BASE + "/v1/chat/completions"
TOOLS = [{"type": "function", "function": {"name": "delete_database",
                                           "parameters": {"type": "object"}}}]


def configure(**kw):
    payload = {"upstream": mock.base_url, "api_key": "sk-test", "budget_usd": 5.00,
               "check_policy": None, "fallback_model": "", "loop_detection": False,
               "blocked_tools": ["delete_*", "drop_*", "refund"],
               "allowed_tools": [], "tool_firewall_mode": "block"}
    payload.update(kw)
    return httpx.post(BASE + "/config", json=payload).json()


def call(model, stream=False, tools=TOOLS):
    return httpx.post(URL, json={"model": model, "stream": stream, "tools": tools,
                                 "messages": [{"role": "user", "content": "do it"}]},
                      timeout=60)


# ============================================================
print("=== 1) tool distruttivo -> HTTP 403, mai consegnato ===")
configure()
r = call("tool-caller-danger")
print("HTTP", r.status_code)
assert r.status_code == 403, "un tool vietato deve essere bloccato con 403"
j = r.json()
print("message   :", j["error"]["message"])
print("x_spendguard:", {k: j["x_spendguard"][k] for k in ("blocked_tool", "reason")})
assert j["error"]["code"] == "tool_call_blocked"
assert "delete_database" not in json.dumps(j.get("choices", [])), "l'azione non deve trapelare"
assert tool_firewall.blocked_count == 1
print("destructive call blocked  : PASS")

# ============================================================
print("\n=== 2) tool innocuo -> deve passare, con i tool_calls INTATTI ===")
r = call("tool-caller-safe")
print("HTTP", r.status_code)
assert r.status_code == 200
msg = r.json()["choices"][0]["message"]
print("tool_calls:", json.dumps(msg.get("tool_calls"), ensure_ascii=False)[:120])
assert msg.get("tool_calls"), "BUG storico: i tool_calls venivano scartati dal proxy"
assert msg["tool_calls"][0]["function"]["name"] == "get_weather"
assert json.loads(msg["tool_calls"][0]["function"]["arguments"])["target"] == "production"
print("safe tool passes through  : PASS")
print("tool_calls no longer lost : PASS")

# ============================================================
print("\n=== 3) ALLOW-LIST (default-deny): piu' forte della deny-list ===")
configure(blocked_tools=[], allowed_tools=["get_*", "search_*"])
r_ok = call("tool-caller-safe")       # get_weather -> permesso
r_no = call("tool-caller-danger")     # delete_database -> non in lista
print("get_weather:", r_ok.status_code, "| delete_database:", r_no.status_code)
assert r_ok.status_code == 200 and r_no.status_code == 403
assert "not in allow-list" in r_no.json()["error"]["message"]
print("allow-list default-deny   : PASS")

# ============================================================
print("\n=== 4) modalita' OVERRIDE: l'agente puo' correggersi invece di crashare ===")
configure(tool_firewall_mode="override")
r = call("tool-caller-danger")
print("HTTP", r.status_code)
assert r.status_code == 200, "in override il client riceve 200 con una spiegazione"
m = r.json()["choices"][0]["message"]
print("content:", (m["content"] or "")[:110])
assert "blocked by policy" in (m["content"] or "")
assert not m.get("tool_calls"), "l'azione pericolosa deve essere rimossa"
assert r.json()["x_spendguard"]["tool_call_blocked"] is True
print("controlled override works : PASS")

# ============================================================
print("\n=== 5) streaming: il blocco funziona anche in streaming ===")
configure(tool_firewall_mode="block")
r = call("tool-caller-danger", stream=True)
print("HTTP", r.status_code)
assert r.status_code == 403, "con `tools` dichiarati bufferizziamo -> 403 pulito"
print("streaming request blocked : PASS")

# senza `tools` dichiarati resta streaming puro (nessuna regressione di latenza)
r = httpx.post(URL, json={"model": "runaway-reasoner", "stream": True,
                          "messages": [{"role": "user", "content": "hi"}]}, timeout=60)
assert r.status_code == 200 and "data:" in r.text
print("plain streaming untouched : PASS")

# ============================================================
print("\n=== 6) regex e pattern malformati ===")
configure(blocked_tools=["re:^(drop|truncate)_", "[malformed"])
r = httpx.post(URL, json={"model": "tool-caller-safe", "stream": False, "tools": TOOLS,
                          "messages": [{"role": "user", "content": "x"}]}, timeout=60)
assert r.status_code == 200, "un pattern malformato non deve rompere il proxy"
print("regex ok, bad pattern safe: PASS")

# ============================================================
print("\n=== 7) firewall spento -> tutto passa ===")
configure(blocked_tools=[], allowed_tools=[])
r = call("tool-caller-danger")
assert r.status_code == 200
assert r.json()["choices"][0]["message"]["tool_calls"][0]["function"]["name"] == "delete_database"
print("firewall off passes all   : PASS")

print("\nALL ACTION FIREWALL CHECKS PASSED \u2713")
server.should_exit = True