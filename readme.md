# SpendGuard

**A spend and action firewall for autonomous LLM agents.** It sits between your agent and any OpenAI-compatible provider and decides, before each call goes out, whether it should happen at all.

```python
client = OpenAI(base_url="http://127.0.0.1:8900/v1", api_key="your-key")
```

One line. No SDK, no code changes, any language that can set a base URL.

---

## Why a proxy

A limit written into your prompt is a suggestion. The agent's own reasoning can see it, work around it, or decide it doesn't apply. A limit enforced in a separate process, before the request leaves, is a rule — there is nothing for the agent to negotiate with.

That's the whole design. Everything below follows from it.

Observability tools tell you what happened after the money is gone. This is not that.

---

## Start in shadow mode

**Nobody should put an unknown proxy in front of their API key, including this one.** So don't. Start in shadow mode: every check runs, **nothing is blocked**, and you get a report of what *would* have been stopped and what that *would* have cost.

Flip the switch in the dashboard, or:

```bash
curl -X POST localhost:8900/config -H "content-type: application/json" \
  -d '{"enforcement_mode": "shadow"}'
```

Run it alongside your agent for a day, then read `GET /incidents`. If the numbers are zero, you've proven this solves a problem you don't have — which is worth knowing, and cheaper to find out this way.

The spend cap stays enforced even in shadow mode. Simulating that one would mean actually spending money you didn't authorise.

---

## Where your API key goes

Worth being explicit, since this thing sits in your critical path.

- It runs **locally**, on `127.0.0.1`. Nothing is sent anywhere except to the provider you configured.
- Your key is held **in memory only** for the life of the process. It is never written to disk, never logged, and never returned by any endpoint — `GET /config` reports whether a key is set, not what it is.
- If your agent sends its own `Authorization` header, that header is forwarded upstream untouched and SpendGuard doesn't store it at all.
- The whole thing is one Python file you can read in a sitting. Read it before you trust it.

---

## Quick start

```bash
pip install -r requirements.txt
python spend_proxy.py          # opens http://127.0.0.1:8900
```

On Windows, double-click `START_WINDOWS.bat`.

In the dashboard: pick your provider, paste your key, set a budget, turn on the checks you want. Then point your agent at `http://127.0.0.1:8900/v1`.

**No key handy?** The Run panel has five simulated scenarios — runaway spend, stuck loop, failing output, rescue by fallback, destructive action — that run against a built-in fake provider. No key, no real spend.

---

## The five checks

Ordered by how cheap they are to evaluate. The first one costs nothing at all.

### 1. Loop detection

Traditional proxies are stateless: every request looks new, so none of them can tell a working agent from one stuck in a `while True`. SpendGuard keeps a short per-key memory (the key itself is hashed, never stored) and breaks the circuit when the agent repeats the same **move**.

The first version hashed the prompt text. Developers on r/AI_Agents pointed out — three of them, independently — why that fails: an agent that's stuck doesn't resend an identical prompt, it *rewords* the same failing request each time, so a text hash never matches and the loop runs forever. Matching now uses three signals:

| Signal | What it catches |
|---|---|
| **Action** | Hash of the last tool call, name plus arguments. The reliable one. |
| **Fuzzy prompt** | Token overlap, for trivial rewordings. Best-effort. |
| **Exact** | Identical conversation. The easy case. |

When both requests carry a tool call, **the action decides in both directions**. Same arguments means a loop; different arguments means different work, no matter how similar the wording. This matters more than it sounds: an agent working through a queue often sends nearly identical text (`"process the next record"`) with different arguments each time. Blocking that would be worse than the bug it fixes.

For the same reason, requests differing on any token containing digits — `item 1` vs `item 2`, `account 1204` vs `account 1205` — are never treated as a loop.

The breaker **re-closes by itself** once the agent changes its move. It made progress; it shouldn't stay locked out.

Returns `429`, with the call never forwarded.

### 2. Spend cap

Two enforcement points:

- **Pre-flight** — the call is priced before it's sent. If it can't fit in the remaining budget, `402`, nothing spent.
- **Mid-stream** — cost is metered token by token, and the connection is cut the instant the *next* token would cross the line. Not after.

Concurrency-safe: parallel agents share the budget under a lock, so the total never exceeds the cap.

**Per-run budget.** A session cap alone has a failure mode a developer described from experience: one runaway run eats the whole day's budget, and every run after it fails for an unrelated reason. So each run can have its own ceiling.

A "run" is identified by a header your client sends:

```
X-SpendGuard-Run: nightly-import-2026-07-30
```

or, if you send nothing, inferred from the first system + first user message. As the agent works the conversation grows, but those two messages stay put — so they identify the run. Hitting the run ceiling returns `402` with code `run_budget_exceeded`; other runs keep working.

### 3. Quality check

Attach a rule and every completed response is validated against it: `json`, `json_schema`, `regex`, `contains`, `nonempty`.

On failure, SpendGuard can **retry silently on a stronger model**. If that one passes, your agent gets a correct answer and a `200` and never knows anything went wrong. Only when both fail does the breaker open, after N consecutive failures — `409`.

The point isn't validation for its own sake. It's that an agent can burn an entire budget producing garbage while every individual call stays comfortably under the limit.

### 4. Action firewall

Tool calls are inspected on the way back, before your executor sees them. Names matching `blocked_tools` are discarded — glob (`delete_*`) or regex with an `re:` prefix.

An **allow-list** is also supported and is the stronger option: anything not explicitly permitted is denied. A block-list only protects you from names you thought of, and a hallucinating model invents names you didn't.

Two modes:
- `block` — `403`, the call is dropped.
- `override` — the agent gets a normal `200` explaining the tool was denied, so it can correct course instead of crashing on an error.

### 5. Consensus, for irreversible actions

Some spend is recoverable: tokens burned are a lesson. Some isn't: a wrong write, a wrong payment, a deleted table. Irreversible actions deserve a stricter gate than recoverable ones.

In consensus mode a tool call goes to your primary model **and** a second model in parallel — `asyncio.gather`, so latency is the slower of the two, not the sum. The action executes only if both choose the same tool with the same parameters. Strictness is configurable: `exact`, `keys`, or `names`.

**Fail-closed.** If the second model can't be reached, the action does not run. For a safety check, that's the only defensible default.

Disagreement returns `403`.

---

## Configuration

Everything is settable from the dashboard. `POST /config` takes the same fields — send only what you want to change:

```json
{
  "provider": "groq",
  "model": "llama-3.1-8b-instant",
  "api_key": "...",
  "budget_usd": 5.00,
  "per_run_budget_usd": 0.50,

  "loop_detection": true,
  "loop_threshold": 3,
  "loop_window_s": 60,
  "loop_fuzzy_threshold": 0.8,

  "check_policy": {"type": "json_schema", "schema": {}},
  "trip_after": 2,
  "fallback_model": "llama-3.3-70b-versatile",

  "blocked_tools": ["delete_*", "drop_*"],
  "allowed_tools": [],
  "tool_firewall_mode": "block",

  "require_consensus": false,
  "consensus_model": "gpt-4o-mini",
  "consensus_strictness": "exact",

  "enforcement_mode": "shadow",
  "prices": {"my-model": [0.05, 0.08]}
}
```

Providers with presets: `openai`, `groq`, `anthropic`, `ollama`. Anything else, set `upstream` directly.

Environment variables also work: `SPENDGUARD_UPSTREAM`, `SPENDGUARD_API_KEY`, `SPENDGUARD_MODEL`, `SPENDGUARD_BUDGET`, `SPENDGUARD_RUN_BUDGET`, `SPENDGUARD_PORT`, `SPENDGUARD_MAX_TOKENS`, `SPENDGUARD_LOOP_THRESHOLD`, `SPENDGUARD_LOOP_WINDOW`, `SPENDGUARD_BLOCKED_TOOLS`, `SPENDGUARD_FALLBACK`, `SPENDGUARD_CONSENSUS`, `SPENDGUARD_TRIP_AFTER`.

### Prices

The built-in price book is **illustrative and goes stale**. For any model not in it, SpendGuard falls back to an estimate and says so in the activity feed rather than quietly showing you a wrong number. Set real prices with `prices` so the figures mean something.

---

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/chat/completions` | OpenAI-compatible, enforced |
| `GET` | `/stats` | budget, checks, counters |
| `GET` | `/incidents` | shadow-mode report |
| `GET` | `/events` | live SSE event stream |
| `GET` | `/health` | liveness |
| `GET` `POST` | `/config` | read / update configuration |

Control endpoints reject cross-origin POSTs, so a web page open in your browser can't reconfigure your proxy. `/v1` is exempt — agents legitimately run inside pages.

---

## Tests

Ten suites, all against the built-in fake provider. No key, no spend.

```bash
python test_proxy.py        # spend cap, mid-stream kill, 402
python test_concurrency.py  # parallel agents never exceed the cap
python test_loop.py         # loop detection, per-key isolation, time window
python test_loop_v2.py      # action matching, per-run budget, batch-worker safety
python test_quality.py      # quality breaker
python test_fallback.py     # quality-triggered fallback
python test_tools.py        # action firewall, allow-list, override mode
python test_consensus.py    # consensus + shadow mode
python test_config.py       # configuration, error propagation
python test_hardening.py    # truncation, CSRF, pricing warnings
```

Before publishing anything from a fork: `python scan_secrets.py` — scans the working tree **and the git history** for keys, personal paths and emails.

---

## What this doesn't do yet

Written down deliberately. Some of these are gaps developers found and I haven't closed.

**Not built:**

- **No ceiling on input volume.** Every check here keys on repetition. A developer described a run where an upstream source changed and volume went up tenfold with every single call unique and legitimate — nothing in SpendGuard would fire. He found out from a quota alert at 158% of plan.
- **No detection of runs that are suspiciously *cheap*.** A run costing a tenth of normal is often a retrieval that came back empty and a model that summarised over nothing. It looks fine everywhere. Both this and the point above need the same missing piece: a historical baseline of what a normal run looks like.
- **No queryable record of stopped runs.** SpendGuard returns a loud `429`/`402`/`403`, but there's no log a health check can consume. For an agent running unattended, a run stopped by policy and a run that had nothing to do look identical the next morning — no output, no error, green job. Trading a bill you'd have noticed for a data gap you won't is the wrong trade, and closing it is next.
- **No backfill record.** When a run is cut mid-stream, whatever it was supposed to process is simply missing, and no later run picks it up.

**Known constraints:**

- **No persistence.** Restart the proxy and budgets start over. Fine for one developer; not a multi-tenant service.
- **One global session budget**, not per-agent. Loop detection is already per-key; the session budget isn't.
- **Tested against OpenAI-compatible endpoints.** OpenAI and Groq are the tested paths. Others are best-effort.
- **Quality checks and consensus buffer the response.** Streaming is untouched otherwise, but you can't un-send tokens, so validating means briefly holding them.
- **Fuzzy prompt matching is best-effort.** Heavier paraphrases with no tool call attached can still slip past. The action signal is the one to rely on.
- **Loop detection can catch deliberate retries.** An agent intentionally retrying the same action after a transient failure will trip it at the threshold. Tune it, or run shadow mode first to see whether it matters for your traffic.

---

## Files

| File | What it is |
|---|---|
| `spend_proxy.py` | the proxy — all five checks |
| `dashboard.html` | the live dashboard it serves |
| `checks.py` | output validators for the quality check |
| `spend_guard.py` | optional Python wrapper (library form of the spend cap) |
| `mock_llm_server.py` | fake OpenAI-compatible provider for demos and tests |
| `try_it_real.py` | send real calls through the proxy (reads its key from `.env`) |
| `scan_secrets.py` | pre-publish secret scanner |

---

## Credit

The loop detector matches on tool calls instead of prompt text, and has a per-run budget, because developers on r/AI_Agents took the time to explain what was wrong with the first version. The gaps listed above are theirs too. If you find another one, open an issue.

MIT licensed.