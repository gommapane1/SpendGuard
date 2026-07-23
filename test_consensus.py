"""Verifica il Consensus Firewall (doppia chiave) e lo Shadow Mode."""
import threading, time, json
import httpx, uvicorn
from mock_llm_server import MockServerThread
import spend_proxy
from spend_proxy import enforcement, consensus_verdict

mock = MockServerThread(port=8931); mock.start(); mock.wait_until_ready()
cfg = uvicorn.Config(spend_proxy.app, host="127.0.0.1", port=8900, log_level="error")
server = uvicorn.Server(cfg); server.install_signal_handlers = lambda: None
threading.Thread(target=server.run, daemon=True).start()
while not getattr(server, "started", False): time.sleep(0.05)

BASE = "http://127.0.0.1:8900"
URL = BASE + "/v1/chat/completions"
TOOLS = [{"type": "function", "function": {"name": "refund_order",
                                           "parameters": {"type": "object"}}}]


def configure(**kw):
    payload = {"upstream": mock.base_url, "api_key": "sk-test", "budget_usd": 5.00,
               "check_policy": None, "fallback_model": "", "loop_detection": False,
               "blocked_tools": [], "allowed_tools": [],
               "require_consensus": True, "consensus_model": "consensus-b",
               "consensus_strictness": "exact", "enforcement_mode": "enforce"}
    payload.update(kw)
    return httpx.post(BASE + "/config", json=payload).json()


def call(model="consensus-a", tools=TOOLS):
    return httpx.post(URL, json={"model": model, "stream": False, "tools": tools,
                                 "messages": [{"role": "user", "content": "refund order A-1"}]},
                      timeout=90)


# ============================================================
print("=== 1) i due modelli CONCORDANO -> azione validata (200 OK) ===")
configure(consensus_model="consensus-b")
r = call("consensus-a")
print("HTTP", r.status_code)
assert r.status_code == 200, "con accordo l'azione deve passare"
j = r.json()
sg = j["x_spendguard"]
print("consensus     :", sg["consensus"], "|", sg["consensus_reason"])
print("tool eseguito :", j["choices"][0]["message"]["tool_calls"][0]["function"]["name"])
print("costo totale  : $%.6f (somma dei due modelli)" % sg["combined_cost_usd"])
assert sg["consensus"] == "passed"
assert j["choices"][0]["message"]["tool_calls"][0]["function"]["name"] == "refund_order"
print("agreement -> executed     : PASS")

# ============================================================
print("\n=== 2) DISACCORDO sul tool (uno allucina) -> HTTP 403 ===")
configure(consensus_model="consensus-hallucinating")
r = call("consensus-a")
print("HTTP", r.status_code)
assert r.status_code == 403
j = r.json()
print("message :", j["error"]["message"][:105])
print("primary :", j["x_spendguard"]["primary_action"],
      "| secondary:", j["x_spendguard"]["secondary_action"])
assert j["error"]["code"] == "consensus_failed"
assert "Consensus Failed" in j["error"]["message"]
assert "tool_calls" not in json.dumps(j.get("choices", []))
print("disagreement -> blocked   : PASS")

# ============================================================
print("\n=== 3) stesso tool ma PARAMETRI diversi -> bloccato ===")
configure(consensus_model="consensus-divergent")
r = call("consensus-a")
print("HTTP", r.status_code, "|", r.json()["x_spendguard"]["disagreement"][:90])
assert r.status_code == 403, "argomenti diversi = disaccordo (un refund da 50 non e' uno da 5000)"
print("param mismatch caught     : PASS")

# con strictness 'names' invece basta il nome del tool
configure(consensus_model="consensus-divergent", consensus_strictness="names")
r = call("consensus-a")
print("strictness 'names' ->", r.status_code)
assert r.status_code == 200
print("strictness configurable   : PASS")

# ============================================================
print("\n=== 4) FAIL-CLOSED: secondo modello irraggiungibile -> non si esegue ===")
configure(consensus_model="consensus-b", consensus_strictness="exact")
httpx.post(BASE + "/config", json={"upstream": "http://127.0.0.1:9/v1", "reset": False})
r = call("consensus-a")
print("HTTP", r.status_code)
assert r.status_code in (403, 502), "senza secondo parere l'azione non deve passare"
print("fail-closed on error      : PASS")

# ============================================================
print("\n=== 5) PARALLELISMO: la latenza non raddoppia ===")
configure(consensus_model="consensus-b")
t0 = time.monotonic(); call("consensus-a"); t_dual = time.monotonic() - t0
configure(require_consensus=False)
t0 = time.monotonic(); call("consensus-a"); t_single = time.monotonic() - t0
print("una chiamata: %.2fs | con consenso: %.2fs (somma sarebbe ~%.2fs)"
      % (t_single, t_dual, t_single * 2))
assert t_dual < t_single * 1.8, "asyncio.gather deve evitare il raddoppio della latenza"
print("parallel, no 2x latency   : PASS")

# ============================================================
print("\n=== 6) nessun tool richiesto -> il consenso non interferisce ===")
configure(consensus_model="consensus-b")
r = httpx.post(URL, json={"model": "good-model", "stream": False,
                          "messages": [{"role": "user", "content": "hi"}]}, timeout=60)
print("senza tools ->", r.status_code)
assert r.status_code == 200
print("no tools, no interference : PASS")

# ============================================================
print("\n=== 7) SHADOW MODE: registra senza bloccare, misura il risparmio ===")
configure(consensus_model="consensus-hallucinating", enforcement_mode="shadow")
before = enforcement.stats()
r = call("consensus-a")
print("HTTP", r.status_code, "(in shadow non blocca)")
assert r.status_code == 200, "in shadow la chiamata passa"
assert r.json()["x_spendguard"]["consensus"] == "failed_shadow"
st = enforcement.stats()
print("would_block :", st["shadow_would_block"], "| saved_usd: $%.6f" % st["shadow_saved_usd"])
assert st["shadow_would_block"] > before["shadow_would_block"]
assert st["shadow_saved_usd"] > 0, "il risparmio deve essere misurato sul costo reale"
print("shadow records, not blocks: PASS")

rep = httpx.get(BASE + "/incidents").json()
print("referto:", rep["incidents"][0]["kind"], "->", rep["incidents"][0]["reason"][:70])
assert rep["incidents"] and rep["incidents"][0]["kind"] == "consensus"
print("incident report available : PASS")

# shadow copre anche il loop detector
configure(require_consensus=False, loop_detection=True, enforcement_mode="shadow")
body = {"model": "good-model", "stream": False,
        "messages": [{"role": "user", "content": "same prompt every time"}]}
codes = [httpx.post(URL, json=body, timeout=60).status_code for _ in range(4)]
print("loop in shadow ->", codes)
assert all(c == 200 for c in codes), "in shadow il loop non viene bloccato"
kinds = {i["kind"] for i in httpx.get(BASE + "/incidents").json()["incidents"]}
assert "agent_loop" in kinds, "ma deve essere registrato"
print("shadow covers loop too    : PASS")

# e in enforce torna a bloccare
configure(loop_detection=True, enforcement_mode="enforce", require_consensus=False)
codes = [httpx.post(URL, json=body, timeout=60).status_code for _ in range(4)]
print("loop in enforce ->", codes)
assert 429 in codes
print("enforce still blocks      : PASS")

print("\nALL CONSENSUS + SHADOW CHECKS PASSED \u2713")
server.should_exit = True