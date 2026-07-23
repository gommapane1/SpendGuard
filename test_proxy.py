"""Validazione del proxy: enforcement diretto + percorso dashboard. Output sintetico."""
import threading, time, json
import httpx, uvicorn

from mock_llm_server import MockServerThread
import spend_proxy

# --- avvio mock + proxy in thread ---
mock = MockServerThread(port=8931); mock.start(); mock.wait_until_ready()
spend_proxy.UPSTREAM_BASE_URL = mock.base_url
cfg = uvicorn.Config(spend_proxy.app, host="127.0.0.1", port=8900, log_level="error")
server = uvicorn.Server(cfg); server.install_signal_handlers = lambda: None
threading.Thread(target=server.run, daemon=True).start()
while not getattr(server, "started", False): time.sleep(0.05)

budget = spend_proxy.budget
print(f"budget = ${budget.limit:.4f}\n--- driving the proxy directly (streaming) ---")

url = "http://127.0.0.1:8900/v1/chat/completions"
body = {"model": "runaway-reasoner",
        "messages": [{"role": "user", "content": "reason forever, never stop"}],
        "stream": True}

saw_halt = False
saw_402 = False
with httpx.Client(timeout=None) as c:
    for i in range(1, 60):
        with c.stream("POST", url, json=body) as r:
            if r.status_code == 402:
                saw_402 = True
                err = json.loads(r.read().decode())["error"]["code"]
                print(f"call {i:>2}: HTTP 402  ({err})  -> loop stopped before sending")
                break
            halted = False
            for line in r.iter_lines():
                if line.startswith("data:") and '"finish_reason": "budget_halt"' in line:
                    halted = True
            if halted: saw_halt = True
            print(f"call {i:>2}: 200  {'KILL-SWITCH (cut mid-stream)' if halted else 'ok'}"
                  f"   spent ${budget.spent:.4f} / ${budget.limit:.4f}")

print("\n--- assertions ---")
print(f"final spent            : ${budget.spent:.4f}")
assert budget.spent <= budget.limit + 1e-9, "OVERSPEND: spent exceeded budget!"
print("spent <= budget        : PASS")
assert saw_halt, "expected at least one mid-stream kill"
print("mid-stream kill fired  : PASS")
assert saw_402, "expected a 402 refusal once budget exhausted"
print("402 refusal fired      : PASS")

# --- percorso dashboard: reset, unleash, raccolta eventi ---
print("\n--- dashboard path (unleash + /events) ---")
httpx.post("http://127.0.0.1:8900/demo/reset")
kinds = {}
stop = time.time() + 25
with httpx.Client(timeout=None) as c:
    # apre lo stream eventi e poi scatena l'agente
    with c.stream("GET", "http://127.0.0.1:8900/events") as es:
        httpx.post("http://127.0.0.1:8900/demo/unleash")
        for line in es.iter_lines():
            if line.startswith("data:"):
                try: ev = json.loads(line[5:].strip())
                except Exception: continue
                kinds[ev["kind"]] = kinds.get(ev["kind"], 0) + 1
                if ev["kind"] == "loop_stopped": break
            if time.time() > stop: break

print("event kinds seen:", {k: kinds[k] for k in sorted(kinds)})
for needed in ("call_done", "halt", "loop_stopped"):
    assert needed in kinds, f"missing '{needed}' event from dashboard path"
print("dashboard events OK    : PASS")
print(f"\nfinal (dashboard run)  : {spend_proxy.budget.as_dict()}")
print("\nALL CHECKS PASSED ✓")
server.should_exit = True