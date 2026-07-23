"""Verifica il differenziatore: il circuit-breaker di QUALITA'.
L'agente riceve output invalido (prosa dove serviva JSON) -> dopo N fallimenti
il breaker scatta e rifiuta (409), avendo speso una frazione minima del budget."""
import threading, time, json
import httpx, uvicorn
from mock_llm_server import MockServerThread
import spend_proxy

mock = MockServerThread(port=8931); mock.start(); mock.wait_until_ready()
spend_proxy.UPSTREAM_BASE_URL = mock.base_url
cfg = uvicorn.Config(spend_proxy.app, host="127.0.0.1", port=8900, log_level="error")
server = uvicorn.Server(cfg); server.install_signal_handlers = lambda: None
threading.Thread(target=server.run, daemon=True).start()
while not getattr(server, "started", False): time.sleep(0.05)

BASE = "http://127.0.0.1:8900"
budget = spend_proxy.budget

# budget GENEROSO: un semplice cap NON fermerebbe l'agente qui.
httpx.post(BASE + "/config/budget", json={"budget_usd": 5.00})
# policy: l'output deve essere JSON {name:str, value:number}. Il mock manda prosa -> fallira'.
httpx.post(BASE + "/config/check", json={
    "policy": {"type": "json_schema",
               "schema": {"type": "object", "required": ["name", "value"],
                          "properties": {"name": {"type": "string"}, "value": {"type": "number"}}}},
    "trip_after": 2})
print("budget $5.00, quality check = JSON schema {name, value}, trip_after = 2\n")

body = {"model": "runaway-reasoner",
        "messages": [{"role": "user", "content": "Return ONLY JSON {name, value}."}],
        "stream": True}

saw_409 = False
with httpx.Client(timeout=None) as c:
    for i in range(1, 12):
        with c.stream("POST", BASE + "/v1/chat/completions", json=body) as r:
            if r.status_code == 409:
                saw_409 = True
                code = json.loads(r.read().decode())["error"]["code"]
                print(f"call {i:>2}: HTTP 409  ({code})  -> quality breaker OPEN, refused before spending")
                break
            for _ in r.iter_lines():
                pass
        q = spend_proxy._quality_stats()
        print(f"call {i:>2}: 200  output invalid  (consecutive fails now {spend_proxy._breaker('runaway-reasoner')['fails']}, "
              f"breaker {'OPEN' if q['quality_tripped'] else 'closed'})   spent ${budget.spent:.4f} / $5.0000")

print("\n--- assertions ---")
print(f"spent when stopped     : ${budget.spent:.4f}  (of $5.00 budget)")
assert saw_409, "expected a 409 once the quality breaker tripped"
print("quality 409 fired      : PASS")
assert spend_proxy._quality_stats()["quality_tripped"], "breaker should be tripped"
print("breaker tripped        : PASS")
assert budget.spent < 0.50, "should have stopped for cents, not burned the budget"
print("stopped for cents      : PASS  (a plain cap would have let it burn toward $5)")

# la route torna utilizzabile dopo un reset
httpx.post(BASE + "/demo/reset")
assert not spend_proxy._quality_stats()["quality_tripped"]
print("reset re-closes breaker: PASS")

# --- demo qualita' end-to-end via bottone ---
print("\n--- quality demo (unleash_quality + events) ---")
kinds = {}
stop = time.time() + 25
with httpx.Client(timeout=None) as c:
    with c.stream("GET", BASE + "/events") as es:
        httpx.post(BASE + "/demo/unleash_quality")
        for line in es.iter_lines():
            if line.startswith("data:"):
                try: ev = json.loads(line[5:].strip())
                except Exception: continue
                kinds[ev["kind"]] = kinds.get(ev["kind"], 0) + 1
                if ev["kind"] == "loop_stopped":
                    print("stopped_by =", ev.get("stopped_by"))
                    break
            if time.time() > stop: break

print("event kinds:", {k: kinds[k] for k in sorted(kinds)})
for needed in ("quality_fail", "quality_trip", "loop_stopped"):
    assert needed in kinds, f"missing '{needed}' event"
print("quality demo events OK : PASS")

print("\nALL QUALITY CHECKS PASSED \u2713")
server.should_exit = True