"""Verifica le correzioni nate dal feedback degli sviluppatori su Reddit:
  A. il loop si riconosce dall'AZIONE (tool + argomenti), non dal testo
     -> l'agente che riformula la stessa query fallita ora viene preso
  B. budget PER RUN separato da quello di sessione
     -> una run impazzita non prosciuga il budget delle altre
"""
import threading, time, json
import httpx, uvicorn
from mock_llm_server import MockServerThread
import spend_proxy
from spend_proxy import LoopDetector, RunTracker, loop_detector, run_tracker

mock = MockServerThread(port=8931); mock.start(); mock.wait_until_ready()
cfg = uvicorn.Config(spend_proxy.app, host="127.0.0.1", port=8900, log_level="error")
server = uvicorn.Server(cfg); server.install_signal_handlers = lambda: None
threading.Thread(target=server.run, daemon=True).start()
while not getattr(server, "started", False): time.sleep(0.05)

BASE = "http://127.0.0.1:8900"
URL = BASE + "/v1/chat/completions"


def configure(**kw):
    p = {"upstream": mock.base_url, "api_key": "sk-test", "model": "good-model",
         "budget_usd": 5.00, "check_policy": None, "fallback_model": "",
         "blocked_tools": [], "require_consensus": False, "loop_detection": True,
         "loop_threshold": 3, "loop_window_s": 60, "per_run_budget_usd": None,
         "enforcement_mode": "enforce"}
    p.update(kw)
    return httpx.post(BASE + "/config", json=p).json()


def call(messages, key="sk-a", run=None, model="good-model"):
    h = {"authorization": "Bearer " + key}
    if run:
        h["x-spendguard-run"] = run
    return httpx.post(URL, json={"model": model, "stream": False, "messages": messages},
                      headers=h, timeout=90)


# =====================================================================
print("=== A1) IL DIFETTO SEGNALATO: query riformulata sulla stessa azione ===")
print("    (tre sviluppatori hanno segnalato che l'hash del testo non basta)")
configure()

# L'agente ritenta la STESSA tool call, ma riscrive il testo ogni volta.
def rephrased(attempt_text):
    return [
        {"role": "system", "content": "You are an agent."},
        {"role": "user", "content": attempt_text},
        {"role": "assistant", "tool_calls": [
            {"id": "c1", "type": "function",
             "function": {"name": "fetch_order", "arguments": '{"id": "A-1"}'}}]},
        {"role": "tool", "tool_call_id": "c1", "content": "error: timeout"},
    ]

wordings = [
    "Fetch the order A-1 and retry",
    "Please try fetching order A-1 once more",
    "Attempt to retrieve order A-1 again now",
    "Could you get order A-1, retrying",
]
codes = []
for i, w in enumerate(wordings, 1):
    r = call(rephrased(w))
    codes.append(r.status_code)
    if r.status_code == 429:
        j = r.json()
        print(f"    call {i}: HTTP 429  matched_on = '{j['x_spendguard']['matched_on']}'")
        break
    print(f"    call {i}: HTTP {r.status_code}  (testo diverso ogni volta)")

assert 429 in codes, "il loop riformulato DEVE essere riconosciuto (era il bug)"
assert loop_detector.trips_by_signal.get("same tool call and arguments", 0) >= 1, \
    "deve scattare sul segnale dell'AZIONE, non sul testo"
print("    reworded loop caught          : PASS  <- questo prima passava indenne")
print("    matched on the action, not text: PASS")

# =====================================================================
print("\n=== A2) azione DIVERSA -> non deve scattare ===")
configure()
def action_on(order_id):
    return [
        {"role": "user", "content": "process the next order"},
        {"role": "assistant", "tool_calls": [
            {"id": "c1", "type": "function",
             "function": {"name": "fetch_order", "arguments": '{"id": "%s"}' % order_id}}]},
        {"role": "tool", "tool_call_id": "c1", "content": "ok"},
    ]
codes = [call(action_on("A-%d" % i)).status_code for i in range(1, 6)]
print("    codes:", codes, "(stesso tool, argomenti diversi = lavoro vero)")
assert all(c == 200 for c in codes), "argomenti diversi = lavoro legittimo, non loop"
print("    different args not a loop     : PASS")

# =====================================================================
print("\n=== A3) prompt quasi identici senza tool (segnale fuzzy) ===")
configure()
near = ["summarize the quarterly revenue report",
        "summarize quarterly revenue report",
        "summarize the quarterly revenue report please"]
codes = []
for i, t in enumerate(near, 1):
    r = call([{"role": "user", "content": t}])
    codes.append(r.status_code)
    if r.status_code == 429:
        print(f"    call {i}: HTTP 429  matched_on = '{r.json()['x_spendguard']['matched_on']}'")
        break
print("    codes:", codes)
assert 429 in codes, "riformulazioni banali devono essere riconosciute"
print("    fuzzy rewording caught        : PASS")

# e richieste davvero diverse NON devono scattare
configure()
diff = ["summarize the revenue report", "delete stale cache entries",
        "send the weekly digest email", "rebuild the search index",
        "check disk usage on node 3"]
codes = [call([{"role": "user", "content": t}]).status_code for t in diff]
print("    codes (richieste diverse):", codes)
assert all(c == 200 for c in codes), "richieste diverse non devono mai scattare"
print("    no false positives            : PASS")

# =====================================================================
print("\n=== A4) LAVORATORE A LOTTI: differisce solo l'ID -> mai un loop ===")
print("    (il falso positivo piu' dannoso: bloccare chi sta lavorando davvero)")
configure()
batch = ["process item 1", "process item 2", "process item 3",
         "process item 4", "process item 5", "process item 6"]
codes = [call([{"role": "user", "content": t}]).status_code for t in batch]
print("    codes:", codes)
assert all(c == 200 for c in codes), "differire su un ID = lavoro diverso"

configure()
long_batch = ["analyze the customer account data for account %d and summarize" % i
              for i in (1201, 1202, 1203, 1204, 1205)]
codes = [call([{"role": "user", "content": t}]).status_code for t in long_batch]
print("    codes (prompt lunghi, un solo ID diverso):", codes)
assert all(c == 200 for c in codes), "anche con prompt lunghi l'ID deve distinguere"
print("    batch worker never blocked    : PASS")

print("\n=== B1) BUDGET PER RUN: una run impazzita non tocca le altre ===")
configure(budget_usd=5.00, per_run_budget_usd=0.0002, loop_detection=False)
print("    session budget $5.00, per-run budget $0.0002 (~5 chiamate)")

# run A: martella finche' non esaurisce il SUO budget
stopped_at = None
for i in range(1, 40):
    r = call([{"role": "user", "content": "run A step %d" % i}], run="run-A")
    if r.status_code == 402:
        j = r.json()
        stopped_at = i
        print(f"    run A, call {i}: HTTP 402  code = {j['error']['code']}")
        print(f"                     scope = {j.get('x_spendguard', {}).get('scope')}")
        break
assert stopped_at, "la run A deve essere fermata dal proprio tetto"
assert spend_proxy.budget.spent < 5.00, "il budget di sessione non deve essere esaurito"
print(f"    session spent so far: ${spend_proxy.budget.spent:.4f} / $5.00")
print("    run A stopped by its own cap  : PASS")

# run B deve poter lavorare normalmente
r = call([{"role": "user", "content": "run B step 1"}], run="run-B")
print("    run B, call 1: HTTP", r.status_code)
assert r.status_code == 200, "una run diversa NON deve essere penalizzata"
print("    run B unaffected              : PASS  <- il punto di tutta la feature")

# =====================================================================
print("\n=== B2) run dedotta senza header (stesso primo messaggio) ===")
configure(budget_usd=5.00, per_run_budget_usd=0.0002, loop_detection=False)
first = {"role": "user", "content": "analyse the sales file"}
hit = None
for i in range(1, 40):
    msgs = [first, {"role": "assistant", "content": "step %d" % i}]
    r = call(msgs)
    if r.status_code == 402:
        hit = i
        print(f"    call {i}: HTTP 402 ({r.json()['error']['code']})")
        break
assert hit, "la run dedotta deve essere riconosciuta e limitata"
# una conversazione con un PRIMO messaggio diverso e' un'altra run
r = call([{"role": "user", "content": "a completely different task"}])
print("    diversa prima richiesta ->", r.status_code)
assert r.status_code == 200
print("    inferred run id works         : PASS")

# =====================================================================
print("\n=== B3) disattivato di default: nessuna regressione ===")
configure(per_run_budget_usd=None, loop_detection=False)
codes = [call([{"role": "user", "content": "task %d" % i}]).status_code for i in range(6)]
print("    codes:", codes)
assert all(c == 200 for c in codes)
assert run_tracker.stats()["per_run_limit_usd"] is None
print("    off by default                : PASS")

print("\nALL LOOP-V2 + PER-RUN CHECKS PASSED \u2713")
server.should_exit = True