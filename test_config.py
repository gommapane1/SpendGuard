"""Verifica: (1) il fix del bug 413 -> max_tokens non viene mai gonfiato,
(2) tutta la configurazione passa dalla dashboard via HTTP, senza terminale."""
import threading, time, json
import httpx, uvicorn
import mock_llm_server
from mock_llm_server import MockServerThread
import spend_proxy

mock = MockServerThread(port=8931); mock.start(); mock.wait_until_ready()
cfg = uvicorn.Config(spend_proxy.app, host="127.0.0.1", port=8900, log_level="error")
server = uvicorn.Server(cfg); server.install_signal_handlers = lambda: None
threading.Thread(target=server.run, daemon=True).start()
while not getattr(server, "started", False): time.sleep(0.05)

BASE = "http://127.0.0.1:8900"
print("--- configurazione via dashboard (nessun terminale) ---")

r = httpx.post(BASE + "/config", json={
    "upstream": mock.base_url, "api_key": "sk-test-key",
    "model": "runaway-reasoner", "budget_usd": 1.00}).json()
print("config ->", {k: r[k] for k in ("upstream", "model", "has_key", "limit_usd")})
assert r["has_key"] is True and r["limit_usd"] == 1.00
print("config applied via HTTP    : PASS")

# la chiave non viene MAI restituita al browser
assert "api_key" not in httpx.get(BASE + "/config").json()
print("key never echoed back      : PASS")

print("\n--- BUG FIX 413: max_tokens non deve essere gonfiato ---")
# budget alto: prima calcolavamo max_tokens enorme (-> Groq HTTP 413)
body = {"model": "runaway-reasoner",
        "messages": [{"role": "user", "content": "hello"}], "stream": True}
with httpx.Client(timeout=None) as c:
    with c.stream("POST", BASE + "/v1/chat/completions", json=body) as resp:
        for _ in resp.iter_lines():
            pass
sent = mock_llm_server.LAST_REQUEST.get("max_tokens")
print(f"budget $1.00, client didn't set max_tokens -> proxy sent max_tokens = {sent}")
assert sent is not None and sent <= spend_proxy.DEFAULT_MAX_TOKENS, "max_tokens inflated!"
print("max_tokens capped sanely   : PASS  (was thousands -> Groq 413)")

# se il client CHIEDE un valore, lo rispettiamo (non lo alziamo mai)
body2 = dict(body); body2["max_tokens"] = 32
with httpx.Client(timeout=None) as c:
    with c.stream("POST", BASE + "/v1/chat/completions", json=body2) as resp:
        for _ in resp.iter_lines():
            pass
sent2 = mock_llm_server.LAST_REQUEST.get("max_tokens")
print(f"client asked 32 -> proxy sent {sent2}")
assert sent2 == 32
print("never inflates client's ask: PASS")

print("\n--- l'errore upstream arriva alla dashboard ---")
httpx.post(BASE + "/config", json={"upstream": "http://127.0.0.1:9/v1"})  # porta morta
kinds = []
with httpx.Client(timeout=None) as c:
    with c.stream("GET", BASE + "/events") as es:
        def fire():
            time.sleep(0.4)
            try:
                httpx.post(BASE + "/v1/chat/completions",
                           json={"model": "x", "messages": [{"role": "user", "content": "hi"}]},
                           timeout=20)
            except Exception:
                pass
        threading.Thread(target=fire, daemon=True).start()
        deadline = time.time() + 15
        for line in es.iter_lines():
            if line.startswith("data:"):
                try: ev = json.loads(line[5:].strip())
                except Exception: continue
                kinds.append(ev["kind"])
                if ev["kind"] == "upstream_error":
                    print("dashboard received:", ev["kind"], "->", str(ev.get("detail"))[:60])
                    break
            if time.time() > deadline: break
assert "upstream_error" in kinds, "l'errore del provider deve arrivare alla dashboard"
print("upstream errors surfaced   : PASS")

# e il budget non deve essere addebitato per una chiamata fallita
assert spend_proxy.budget.spent == 0.0, "una chiamata fallita non deve costare"
print("failed call costs nothing  : PASS")

print("\nALL CONFIG/BUGFIX CHECKS PASSED \u2713")
server.should_exit = True