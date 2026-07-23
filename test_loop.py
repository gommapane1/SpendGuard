"""Verifica lo Stateful Loop Detection (rilevatore di loop comportamentale)."""
import threading, time, json
import httpx, uvicorn
from mock_llm_server import MockServerThread
import spend_proxy
from spend_proxy import loop_detector

mock = MockServerThread(port=8931); mock.start(); mock.wait_until_ready()
cfg = uvicorn.Config(spend_proxy.app, host="127.0.0.1", port=8900, log_level="error")
server = uvicorn.Server(cfg); server.install_signal_handlers = lambda: None
threading.Thread(target=server.run, daemon=True).start()
while not getattr(server, "started", False): time.sleep(0.05)

BASE = "http://127.0.0.1:8900"
URL = BASE + "/v1/chat/completions"


def configure(**kw):
    payload = {"upstream": mock.base_url, "api_key": "sk-test", "model": "runaway-reasoner",
               "budget_usd": 5.00, "check_policy": None, "fallback_model": "",
               "loop_detection": True, "loop_threshold": 3, "loop_window_s": 60}
    payload.update(kw)
    return httpx.post(BASE + "/config", json=payload).json()


def call(text, key="sk-agent-A"):
    return httpx.post(URL, json={"model": "runaway-reasoner", "stream": False,
                                 "messages": [{"role": "user", "content": text}]},
                      headers={"authorization": "Bearer " + key}, timeout=120)


# ============================================================
print("=== 1) stesso prompt 3 volte -> blocco alla terza ===")
configure()
codes = []
for i in range(1, 5):
    r = call("check the order status and retry")
    codes.append(r.status_code)
    if r.status_code == 429:
        j = r.json()
        print(f"call {i}: HTTP 429 -> {j['error']['message']}")
        print(f"         x_spendguard: {j['x_spendguard']}")
        break
    print(f"call {i}: HTTP {r.status_code} (forwarded)")
assert codes[:2] == [200, 200], "le prime due devono passare"
assert codes[2] == 429, "la terza ripetizione identica deve essere bloccata"
print("blocks on 3rd identical    : PASS")

spent_at_trip = spend_proxy.budget.spent
r = call("check the order status and retry")
assert r.status_code == 429
assert spend_proxy.budget.spent == spent_at_trip, "una chiamata bloccata non deve costare"
print("blocked call costs $0      : PASS")
print(f"           (budget $5.00, spent only ${spent_at_trip:.4f} before breaking the loop)")

# ============================================================
print("\n=== 2) prompt DIVERSO -> sblocca (l'agente ha fatto progressi) ===")
r = call("now fetch the invoice instead")
print("different prompt ->", r.status_code)
assert r.status_code == 200, "un prompt diverso deve richiudere il breaker"
print("recovers on new prompt     : PASS")

# ============================================================
print("\n=== 3) prompt sempre diversi -> non scatta mai ===")
configure()
codes = [call(f"task number {i}").status_code for i in range(6)]
print("codes:", codes)
assert all(c == 200 for c in codes), "prompt diversi non devono mai attivare il rilevatore"
assert loop_detector.trips == 0
print("no false positives         : PASS")

# ============================================================
print("\n=== 4) isolamento per chiave API ===")
configure()
call("shared prompt", key="sk-agent-A")
call("shared prompt", key="sk-agent-A")
r_a = call("shared prompt", key="sk-agent-A")          # 3a per A -> bloccata
r_b = call("shared prompt", key="sk-agent-B")          # 1a per B -> passa
print("agent A 3rd:", r_a.status_code, "| agent B 1st:", r_b.status_code)
assert r_a.status_code == 429 and r_b.status_code == 200
print("per-key isolation          : PASS")

# la chiave non viene mai memorizzata in chiaro
ids = list(loop_detector._hist.keys())
assert all("sk-" not in i for i in ids), "le chiavi non devono comparire in memoria"
print("keys hashed, never stored  : PASS")

# ============================================================
print("\n=== 5) finestra temporale: ripetizioni lente non scattano ===")
configure(loop_window_s=1.0)
c1 = call("slow repeat").status_code
time.sleep(1.4)
c2 = call("slow repeat").status_code
time.sleep(1.4)
c3 = call("slow repeat").status_code
print("codes:", [c1, c2, c3], "(finestra 1s, chiamate distanziate)")
assert [c1, c2, c3] == [200, 200, 200], "fuori finestra non deve scattare"
print("respects time window       : PASS")

# ============================================================
print("\n=== 6) armonia con budget e quality fallback ===")
SCHEMA = {"type": "object", "required": ["name", "value"],
          "properties": {"name": {"type": "string"}, "value": {"type": "number"}}}
configure(check_policy={"type": "json_schema", "schema": SCHEMA},
          fallback_model="good-model", loop_threshold=3)
# prompt diversi: il loop detector non interferisce, il fallback deve salvare
r1 = call("extract A"); r2 = call("extract B")
print("with fallback ->", r1.status_code, r2.status_code,
      "| saves:", spend_proxy._quality_stats()["fallback_saves"])
assert r1.status_code == 200 and r2.status_code == 200
assert spend_proxy._quality_stats()["fallback_saves"] == 2, "il fallback deve continuare a salvare"
assert loop_detector.trips == 0, "il retry del fallback non deve contare come loop"
print("fallback still rescues     : PASS")
print("fallback retry not counted : PASS")

# e il loop detector scatta comunque, anche con la qualita' attiva
call("stuck prompt"); call("stuck prompt")
r = call("stuck prompt")
print("loop with quality on ->", r.status_code)
assert r.status_code == 429
print("loop wins over quality     : PASS")

# ============================================================
print("\n=== 7) disattivabile ===")
configure(loop_detection=False)
codes = [call("identical always").status_code for _ in range(4)]
print("codes with detection off:", codes)
assert all(c == 200 for c in codes)
print("can be turned off          : PASS")

print("\nALL LOOP DETECTION CHECKS PASSED \u2713")
server.should_exit = True