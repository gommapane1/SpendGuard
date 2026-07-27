# SpendGuard

**A spend & action firewall for autonomous LLM agents.** It sits between your agent and any OpenAI-compatible provider and stops the things that actually go wrong in production — before they cost you money or break something.

Point your agent at it by changing **one line** (`base_url`). No SDK, no code changes, any language.

```python
client = OpenAI(base_url="http://127.0.0.1:8900/v1", api_key="your-key")
```

---

## Why

Observability tools tell you what happened *after* the money is gone. SpendGuard is a **policy engine**: it decides, in real time, whether a call should happen at all.

Five defences, ordered from cheapest to most expensive to evaluate:

| # | Defence | Stops | Response |
|---|---------|-------|----------|
| 1 | **Loop detection** | An agent stuck in `while True` re-sending the identical prompt | `429`, call never forwarded |
| 2 | **Spend cap** | A runaway loop burning your budget | `402` pre-flight, plus a mid-stream kill-switch |
| 3 | **Quality breaker** | An agent that keeps producing invalid output | `409` after N consecutive failures |
| 4 | **Action firewall** | A hallucinated `delete_database` tool call | `403`, discarded before your executor sees it |
| 5 | **Consensus** | A single model confidently choosing a destructive action | `403` unless a second model independently agrees |

Plus **Shadow mode** — run everything in observe-only and see exactly what *would* have been blocked, and what it *would* have saved, before you let it block anything.

---

## Quick start

```bash
pip install -r requirements.txt
python spend_proxy.py          # opens http://127.0.0.1:8900 automatically
```

Windows: double-click `START_WINDOWS.bat`.

In the dashboard: pick your provider, paste your API key, set a budget, flip on the defences you want. Then point your agent at `http://127.0.0.1:8900/v1`.

**No key handy?** The Run panel has five simulated scenarios (runaway spend, stuck loop, failing output, rescue by fallback, destructive action) that run against a built-in fake provider — no key, no real spend.

Your API key lives only in the local process's memory. It is forwarded to your provider and never written to disk.

---

## The defences in detail

### 1. Loop detection (stateful)

Traditional proxies are stateless: every request looks new. SpendGuard keeps a short per-key memory of prompt fingerprints (SHA-256; the API key itself is hashed, never stored) and breaks the circuit when the same prompt repeats N times inside a time window. The breaker **re-closes automatically** when the agent sends a different prompt — it made progress, so it isn't locked out forever.

### 2. Spend cap

Two enforcement points:

- **Pre-flight** — the call is priced before sending. If it can't fit the remaining budget → `402`, nothing spent.
- **Mid-stream** — cost is metered token by token; the instant the next token would cross the cap, the upstream connection is cut.

Concurrency-safe: parallel agents share the budget under a lock, so the total never exceeds the cap.

### 3. Quality breaker + fallback

Attach a rule (`valid JSON`, or a JSON Schema). Every completed response is validated. On failure, SpendGuard can **silently retry with a stronger fallback model** — if that one passes, your agent gets a correct answer and a `200`, and never knows anything went wrong. Only if both fail does the breaker open.

### 4. Action firewall

Tool calls are inspected on the way out. Names matching `blocked_tools` (glob like `delete_*`, or regex with an `re:` prefix) are discarded and never reach your executor. An **allow-list** is also supported and is stronger: anything not explicitly permitted is denied.

Two modes: `block` (`403`) or `override` — the agent receives a normal message explaining the tool was denied, so it can correct itself instead of crashing.

### 5. Consensus (two-key mode)

For high-stakes actions: the request goes to the primary **and** a second model in parallel (`asyncio.gather` — latency is the slower of the two, not the sum). The action executes only if both choose the same tool with the same parameters. Strictness is configurable (`exact` / `keys` / `names`).

**Fail-closed**: if the second model can't be reached, the action does not run.

---

## Shadow mode

Nobody puts a blocking proxy in their critical path without knowing its false-positive rate. In shadow mode SpendGuard evaluates every defence, **blocks nothing**, and reports what it would have done:

- *would have blocked*: how many calls
- *would have saved*: the **real** cost of the calls that only went through because enforcement was off

`GET /incidents` returns the full report.

The spend cap stays enforced even in shadow — simulating it would mean actually spending unauthorised money.

---

## Configuration

Everything is configurable from the dashboard. `POST /config` accepts the same fields:

```json
{
  "provider": "groq",
  "model": "llama-3.1-8b-instant",
  "api_key": "...",
  "budget_usd": 5.00,
  "loop_detection": true,
  "loop_threshold": 3,
  "check_policy": {"type": "json_schema", "schema": {}},
  "fallback_model": "llama-3.3-70b-versatile",
  "blocked_tools": ["delete_*", "drop_*"],
  "require_consensus": true,
  "consensus_model": "gpt-4o-mini",
  "enforcement_mode": "shadow",
  "prices": {"my-model": [0.05, 0.08]}
}
```

Environment variables work too: `SPENDGUARD_UPSTREAM`, `SPENDGUARD_BUDGET`, `SPENDGUARD_PORT`, `SPENDGUARD_MAX_TOKENS`, `SPENDGUARD_LOOP_THRESHOLD`.

### Pricing

The built-in price book is **illustrative and goes out of date**. For any model not in it, SpendGuard falls back to an estimate and **says so** in the activity feed. Set real prices with the `prices` field so your numbers mean something.

---

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/chat/completions` | OpenAI-compatible, enforced |
| `GET` | `/stats` | budget, defences, counters |
| `GET` | `/incidents` | shadow-mode report |
| `GET` | `/events` | live SSE event stream |
| `GET` | `/health` | liveness |
| `POST` | `/config` | update configuration |

Control endpoints reject cross-origin POSTs, so a web page open in your browser cannot reconfigure your proxy. `/v1` is exempt — agents may legitimately run inside a page.

---

## Tests

```bash
python test_proxy.py        # budget cap, mid-stream kill, 402
python test_concurrency.py  # parallel agents never exceed the cap
python test_quality.py      # quality breaker
python test_fallback.py     # quality-triggered fallback
python test_tools.py        # action firewall
python test_loop.py         # stateful loop detection
python test_consensus.py    # consensus + shadow mode
python test_config.py       # configuration, error propagation
python test_hardening.py    # truncation, CSRF, pricing warnings
```

They run against a built-in fake provider, so no API key and no real spend.

Before publishing anything: `python scan_secrets.py` — scans the folder **and the git history** for keys, personal paths and emails.

---

## Honest limitations

- **No persistence.** Restart the proxy and the budget starts over. Fine for a single developer; not yet a multi-tenant service.
- **One global budget**, not per-agent. Loop detection is already per-key; the budget isn't yet.
- **Tested against OpenAI-compatible endpoints.** OpenAI and Groq are the tested paths; other providers are best-effort.
- **Quality rules and consensus buffer the response.** Streaming is untouched for everything else, but you cannot un-send tokens, so validating means holding them briefly.
- **Loop detection can catch legitimate retries.** An agent retrying the same prompt after a transient failure will trip it at the threshold. Tune it, or run shadow mode first to see whether it matters for your workload.

---

## Files

| File | What it is |
|---|---|
| `spend_proxy.py` | the proxy — all five defences |
| `dashboard.html` | the live dashboard it serves |
| `checks.py` | output validators for the quality rule |
| `spend_guard.py` | optional Python wrapper (library form of the spend cap) |
| `mock_llm_server.py` | fake OpenAI-compatible provider used by demos and tests |
| `try_it_real.py` | send real calls through the proxy (reads its key from `.env`) |
| `scan_secrets.py` | pre-publish secret scanner |