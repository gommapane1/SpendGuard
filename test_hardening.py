"""Verifica le correzioni di robustezza."""
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
URL = BASE + "/v1/chat/completions"


def configure(**kw):
    p = {"upstream": mock.base_url, "api_key": "sk-test", "model": "runaway-reasoner",
         "budget_usd": 5.00, "check_policy": None, "fallback_model": "",
         "loop_detection": False, "blocked_tools": [], "require_consensus": False}
    p.update(kw)
    return httpx.post(BASE + "/config", json=p).json()


# ============================================================
print("=== 1) niente piu' troncamento a 512 token ===")
configure(budget_usd=5.00)
httpx.post(URL, json={"model": "runaway-reasoner", "stream": False,
                      "messages": [{"role": "user", "content": "write a long answer"}]},
           timeout=90)
sent = mock_llm_server.LAST_REQUEST.get("max_tokens", "NOT SENT")
print("budget ampio, nessun max_tokens dal client -> il proxy invia:", sent)
assert sent == "NOT SENT", "con budget ampio non dobbiamo imporre alcun limite"
print("no forced truncation      : PASS")

# ma se il budget e' stretto il tetto lo mettiamo noi
configure(budget_usd=0.004)
httpx.post(URL, json={"model": "runaway-reasoner", "stream": False,
                      "messages": [{"role": "user", "content": "hi"}]}, timeout=90)
sent = mock_llm_server.LAST_REQUEST.get("max_tokens")
print("budget stretto -> il proxy invia max_tokens =", sent)
assert isinstance(sent, int) and sent > 0, "con budget stretto serve il tetto"
print("caps only when needed     : PASS")

# e la richiesta esplicita del client viene rispettata
configure(budget_usd=5.00)
httpx.post(URL, json={"model": "runaway-reasoner", "stream": False, "max_tokens": 64,
                      "messages": [{"role": "user", "content": "hi"}]}, timeout=90)
assert mock_llm_server.LAST_REQUEST.get("max_tokens") == 64
print("respects client's request : PASS")

# ============================================================
print("\n=== 2) stream_options non viene piu' inviato ===")
assert "stream_options" not in mock_llm_server.LAST_REQUEST
print("no unsupported field sent : PASS")

# ============================================================
print("\n=== 3) gli argomenti dei tool passano dal kill-switch ===")
# budget minuscolo: la tool call deve essere tagliata, non sforare
configure(budget_usd=0.0006)
r = httpx.post(URL, json={"model": "tool-caller-safe", "stream": False,
                          "tools": [{"type": "function", "function": {"name": "get_weather"}}],
                          "messages": [{"role": "user", "content": "weather?"}]}, timeout=90)
spent = spend_proxy.budget.spent
print("HTTP", r.status_code, "| speso $%.6f / $0.000600" % spent)
assert spent <= 0.0006 + 1e-9, "gli argomenti dei tool non devono sfondare il budget"
print("tool args respect cap     : PASS")

# ============================================================
print("\n=== 4) avviso sui modelli con prezzo stimato ===")
configure(budget_usd=1.00)
kinds = []
with httpx.Client(timeout=None) as c:
    with c.stream("GET", BASE + "/events") as es:
        def fire():
            time.sleep(0.4)
            httpx.post(URL, json={"model": "modello-mai-visto", "stream": False,
                                  "messages": [{"role": "user", "content": "x"}]}, timeout=60)
        threading.Thread(target=fire, daemon=True).start()
        deadline = time.time() + 15
        for line in es.iter_lines():
            if line.startswith("data:"):
                try: ev = json.loads(line[5:].strip())
                except Exception: continue
                kinds.append(ev["kind"])
                if ev["kind"] == "price_warning":
                    print("avviso ricevuto per:", ev["model"])
                    break
            if time.time() > deadline: break
assert "price_warning" in kinds, "un modello sconosciuto deve generare un avviso"
print("unknown price warned      : PASS")

# e si possono impostare i prezzi veri
d = httpx.post(BASE + "/config", json={"prices": {"modello-mai-visto": [0.05, 0.08]}}).json()
assert spend_proxy._prices["modello-mai-visto"] == (0.05, 0.08)
print("custom prices settable    : PASS")

# ============================================================
print("\n=== 5) protezione CSRF sugli endpoint di controllo ===")
r = httpx.post(BASE + "/config", json={"budget_usd": 999},
               headers={"origin": "https://sito-malevolo.example"}, timeout=30)
print("POST /config da altra origine ->", r.status_code)
assert r.status_code == 403, "una pagina esterna non deve poter cambiare la configurazione"
assert spend_proxy.budget.limit != 999
print("cross-origin control blocked: PASS")

# la dashboard locale invece funziona
r = httpx.post(BASE + "/config", json={"budget_usd": 1.5},
               headers={"origin": BASE}, timeout=30)
assert r.status_code == 200 and spend_proxy.budget.limit == 1.5
print("local dashboard still works: PASS")

# e gli agenti su /v1 non sono toccati (possono girare in una pagina web)
r = httpx.post(URL, json={"model": "good-model", "stream": False,
                          "messages": [{"role": "user", "content": "hi"}]},
               headers={"origin": "https://mia-app.example"}, timeout=60)
assert r.status_code == 200, "gli agenti non devono essere bloccati dall'Origin"
print("agents unaffected          : PASS")

# ============================================================
print("\n=== 6) /health ===")
h = httpx.get(BASE + "/health").json()
print("health:", h)
assert h["status"] == "ok"
print("health endpoint            : PASS")

print("\nALL HARDENING CHECKS PASSED \u2713")
server.should_exit = True