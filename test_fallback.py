"""Verifica il Quality-Triggered Fallback (semantic routing).

Scenario: il modello primario risponde prosa (fallisce lo schema JSON).
  1. Con un fallback che risponde bene  -> il client riceve 200 OK con la risposta CORRETTA
  2. Con un fallback che sbaglia anche lui -> dopo N fallimenti scatta il breaker (409)
  3. Se il budget non basta per il fallback -> niente crash, conta come fallimento
"""
import threading, time, json
import httpx, uvicorn
from mock_llm_server import MockServerThread
import spend_proxy

mock = MockServerThread(port=8931); mock.start(); mock.wait_until_ready()
cfg = uvicorn.Config(spend_proxy.app, host="127.0.0.1", port=8900, log_level="error")
server = uvicorn.Server(cfg); server.install_signal_handlers = lambda: None
threading.Thread(target=server.run, daemon=True).start()
while not getattr(server, "started", False): time.sleep(0.05)

BASE = "http://127.0.0.1:8900"
SCHEMA = {"type": "object", "required": ["name", "value"],
          "properties": {"name": {"type": "string"}, "value": {"type": "number"}}}
MSG = [{"role": "user", "content": "Extract name and value as JSON."}]


def configure(**kw):
    payload = {"upstream": mock.base_url, "api_key": "sk-test",
               "model": "runaway-reasoner", "budget_usd": 1.00,
               "check_policy": {"type": "json_schema", "schema": SCHEMA},
               "trip_after": 2}
    payload.update(kw)
    return httpx.post(BASE + "/config", json=payload).json()


def call(stream=False):
    body = {"model": "runaway-reasoner", "messages": MSG, "stream": stream}
    return httpx.post(BASE + "/v1/chat/completions", json=body, timeout=120)


# ============================================================
print("=== 1) fallback BUONO: deve salvare la chiamata ===")
configure(fallback_model="good-model")
r = call()
print("HTTP", r.status_code)
assert r.status_code == 200, "il fallback deve salvare la chiamata -> 200 OK"
j = r.json()
content = j["choices"][0]["message"]["content"]
sg = j["x_spendguard"]
print("model returned :", j["model"])
print("content        :", content)
print("x_spendguard   : fallback_used =", sg.get("fallback_used"),
      "| primary =", sg.get("primary_model"), "-> fallback =", sg.get("fallback_model"))
assert sg.get("fallback_used") is True
assert json.loads(content)["name"] == "sea", "il contenuto deve essere il JSON valido del fallback"
st = spend_proxy._quality_stats()
assert st["fallback_saves"] == 1 and st["quality_tripped"] is False
print("client rescued with 200 OK : PASS")
print("breaker NOT tripped        : PASS")

# ripetuto: continua a salvare, il breaker resta chiuso
call(); call()
st = spend_proxy._quality_stats()
print(f"after 3 calls -> saves={st['fallback_saves']}, tripped={st['quality_tripped']}")
assert st["fallback_saves"] == 3 and st["quality_tripped"] is False
print("repeated rescues, no trip  : PASS")

# ============================================================
print("\n=== 2) fallback che sbaglia anche lui: breaker deve scattare ===")
configure(fallback_model="runaway-reasoner-2")   # modello finto = prosa -> fallisce
codes = []
for i in range(4):
    r = call()
    codes.append(r.status_code)
    if r.status_code == 409:
        print(f"call {i+1}: HTTP 409 -> {json.loads(r.text)['error']['code']}")
        break
    print(f"call {i+1}: HTTP {r.status_code} (quality failed, breaker still closed)")
assert 409 in codes, "con entrambi i modelli falliti il breaker deve aprirsi (409)"
assert spend_proxy._quality_stats()["quality_tripped"] is True
print("both fail -> 409           : PASS")

# la chiamata successiva e' rifiutata prima di spendere
spent_before = spend_proxy.budget.spent
r = call()
assert r.status_code == 409 and spend_proxy.budget.spent == spent_before
print("next call refused, $0 spent: PASS")

# ============================================================
print("\n=== 3) budget insufficiente per il fallback: nessun crash ===")
configure(fallback_model="good-model", budget_usd=0.008)  # basta a malapena per 1 chiamata
r1 = call()
r2 = call()
print("HTTP:", r1.status_code, r2.status_code, "| spent",
      round(spend_proxy.budget.spent, 5), "/ 0.008")
assert r1.status_code in (200, 402, 409) and r2.status_code in (200, 402, 409)
assert spend_proxy.budget.spent <= 0.008 + 1e-9, "il fallback non deve sfondare il budget"
print("fallback respects budget   : PASS")

# ============================================================
print("\n=== 4) streaming: il client riceve la risposta CORRETTA ===")
configure(fallback_model="good-model", budget_usd=1.00)
r = call(stream=True)
text = ""
for line in r.text.splitlines():
    if line.startswith("data:") and "[DONE]" not in line:
        try:
            ch = json.loads(line[5:].strip())
        except Exception:
            continue
        text += ((ch.get("choices") or [{}])[0].get("delta") or {}).get("content", "")
print("streamed content:", text)
assert json.loads(text)["value"] == 42, "anche in streaming deve arrivare il JSON del fallback"
print("streaming rescued too      : PASS")

# ============================================================
print("\n=== 5) senza fallback configurato: comportamento invariato ===")
configure(fallback_model="")
codes = [call().status_code for _ in range(3)]
print("codes:", codes)
assert 409 in codes, "senza fallback il breaker deve aprirsi come prima"
print("no-fallback unchanged      : PASS")

print("\nALL FALLBACK CHECKS PASSED \u2713")
server.should_exit = True