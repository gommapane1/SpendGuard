# SpendGuard

> A real-time spend firewall for autonomous LLM agents. It stops your agent **before** it bankrupts you — not after.

*(SpendGuard is a working name — easy to change.)*

Observability tools show you the bill *after* the money is gone. SpendGuard sits on the wire between your agent and any **OpenAI-compatible** provider and enforces a hard dollar budget at the two moments money actually leaves your pocket:

1. **Pre-flight** — before a single token is sent, it prices the call. If the input alone would blow the remaining budget, the call is **refused** (the proxy answers `HTTP 402 Payment Required`). Nothing is spent. This is what stops a runaway loop: the next call never fires.
2. **Mid-stream** — it forces streaming and meters cost token-by-token. The instant the *next* token would cross the cap, it **cuts the stream** and closes the upstream connection. The kill-switch. This is what saves you when a provider ignores `max_tokens` (reasoning models, some OSS endpoints).

Spend never exceeds the budget — validated end-to-end (`test_proxy.py`).

## The quality circuit-breaker — what a plain cap can't do

A spend cap only knows *how much* you spent. It happily lets an agent burn your
whole budget producing **garbage** in a loop — every call is "under budget," so
nothing stops it until the money is gone.

SpendGuard also watches *whether the output is correct*. You attach a check
(JSON Schema, regex, required substring, non-empty…); SpendGuard validates every
completed response, and after N consecutive failures it **opens the breaker** and
refuses further calls on that route with `HTTP 409` — before spending. Stop
paying an agent to fail.

```python
# require the model to return valid JSON matching a schema; trip after 2 failures
requests.post("http://localhost:8900/config/check", json={
    "policy": {"type": "json_schema", "schema": {
        "type": "object", "required": ["name", "value"],
        "properties": {"name": {"type": "string"}, "value": {"type": "number"}}}},
    "trip_after": 2,
})
```

In the demo, an agent that keeps returning prose instead of JSON is stopped after
**cents** of a **$5** budget — money a plain cap would have let it burn. Validated
end-to-end (`test_quality.py`). This is the part LiteLLM and Helicone don't do.

## Two ways to use it

### 1. Proxy — zero code change, any language *(flagship)*

Put SpendGuard in front of your agent and change **one line**: the `base_url`. No SDK, no code change, works from any language.

```
# before:  base_url = https://api.openai.com/v1
# after:   base_url = http://localhost:8900/v1      ← SpendGuard proxy
```

When the budget is exhausted the proxy returns a clean `HTTP 402`. Point the proxy at your real provider with one env var:

```bash
SPENDGUARD_UPSTREAM=https://api.groq.com/openai/v1 python3 run_demo.py
```

The proxy forwards your `Authorization` header upstream, so your real key goes straight to the provider — SpendGuard never stores it.

### 2. Library — Python wrapper

```python
from openai import OpenAI
from spend_guard import SpendGuard, BudgetExceededError

guard = SpendGuard(OpenAI(), budget_usd=5.00)
try:
    r = guard.create(model="gpt-4o-mini", messages=[{"role": "user", "content": "hi"}])
    print(r.text, "| $", round(r.cost_usd, 6))
except BudgetExceededError as e:
    print("blocked before spending:", e)
print(guard.summary())
```

`guard.spent_usd` / `guard.remaining_usd` are live at any moment; every call is recorded in `guard.ledger`.

## How to run the demo

You'll see it live in a browser: an agent stuck in a reasoning loop, the spend climbing toward the cap, the kill-switch tripping, and a 402 stopping the loop.

```bash
# 1. put all the files in one folder, then from inside that folder:
pip install -r requirements.txt

# 2. one command starts everything (fake provider + proxy + dashboard):
python3 run_demo.py
#   (on Windows use:  python run_demo.py)

# 3. open the URL it prints — http://127.0.0.1:8900 — and click
#    "Unleash runaway agent".
```

No real API key and no real spend: the demo uses a **fake** OpenAI-compatible endpoint that simulates a verbose model ignoring `max_tokens`.

Prefer the terminal? `python3 demo_runaway_agent.py` runs the library version and prints the same story as text.

## Prices

The built-in price book (`DEFAULT_PRICES_PER_1M` in `spend_guard.py`) is **illustrative and changes over time**. Override per model:

```python
guard.register_price("gpt-4o", input_per_1m=2.50, output_per_1m=10.00)
```

Unknown models fall back to a deliberately non-zero price so the budget still protects them. Token counting uses `tiktoken` when available (exact for OpenAI models, close for others) and falls back to a heuristic if the tokenizer can't load — a budget firewall should never crash for lack of a tokenizer.

## What this is (and isn't) yet

A deliberately narrow wedge: a **session-level budget kill-switch** that works across OpenAI-compatible providers, in proxy or library form, validated end-to-end against the real `openai` client. Honestly *not yet*:

- **per-task / per-agent budgets** (roadmap) — "this agent can spend $X on *this* task"
- **agent authorization** (roadmap) — which identity may call which service, within which budget (the natural next layer)
- **a quality circuit-breaker** (roadmap) — don't spend the next expensive call if the previous step already failed validation

## Files

- `spend_proxy.py` — the transparent proxy (flagship product).
- `dashboard.html` — live browser dashboard the proxy serves.
- `run_demo.py` — one command to run the whole proxy demo.
- `spend_guard.py` — the library/wrapper form of the firewall.
- `demo_runaway_agent.py` — terminal demo of the library form.
- `mock_llm_server.py` — the fake OpenAI-compatible endpoint used by the demos.
- `test_proxy.py` — end-to-end checks (mid-stream kill, 402, never overspends).