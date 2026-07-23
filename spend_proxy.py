"""
================================================================================
 SpendGuard — real-time spend firewall for autonomous LLM agents
================================================================================

Start it once, configure everything in the browser. No terminal gymnastics.

    python spend_proxy.py          (opens http://127.0.0.1:8900 automatically)

Then in the dashboard: pick your provider, paste your API key, set a budget,
and (optionally) a quality rule. Point your agent's base_url at the proxy and
every call is enforced:

  * PRE-FLIGHT   — price the call before sending; if it can't fit the remaining
                   budget, answer HTTP 402 and spend nothing.
  * MID-STREAM   — meter token-by-token; cut the stream the instant the next
                   token would cross the cap.
  * QUALITY      — validate each completed output; after N consecutive failures
                   open the breaker and answer HTTP 409. Stop paying an agent
                   that keeps producing garbage.

Your API key lives only in this local process's memory: it is forwarded to the
provider and never written to disk.
"""

from __future__ import annotations

import asyncio
import fnmatch
import hashlib
import json
import os
import re
import threading
import time
import webbrowser
from collections import deque
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from spend_guard import _count_tokens, DEFAULT_PRICES_PER_1M, FALLBACK_PRICE_PER_1M
from checks import run_check

PROXY_PORT = int(os.environ.get("SPENDGUARD_PORT", "8900"))
PROGRESS_EVERY_TOKENS = 45

# Tetto di sicurezza per max_tokens quando il chiamante non lo specifica.
# BUG FIX: senza questo, calcolavamo max_tokens dal budget (anche migliaia di
# token) e provider come Groq rifiutano la richiesta con HTTP 413 "Request too
# large". Non gonfiamo mai la richiesta: al massimo la riduciamo.
DEFAULT_MAX_TOKENS = int(os.environ.get("SPENDGUARD_MAX_TOKENS", "512"))

_HERE = Path(__file__).resolve().parent
app = FastAPI(title="SpendGuard")

PROVIDER_PRESETS = {
    "openai": "https://api.openai.com/v1",
    "groq": "https://api.groq.com/openai/v1",
    "anthropic": "https://api.anthropic.com/v1",
    "ollama": "http://localhost:11434/v1",
}


# =============================================================================
#  Configurazione runtime (tutta impostabile dalla dashboard)
# =============================================================================

class Config:
    def __init__(self) -> None:
        self.upstream = os.environ.get("SPENDGUARD_UPSTREAM", PROVIDER_PRESETS["openai"])
        self.api_key = os.environ.get("SPENDGUARD_API_KEY", "")   # solo in memoria
        self.model = os.environ.get("SPENDGUARD_MODEL", "gpt-4o-mini")
        # Quality-Triggered Fallback: se l'output del modello primario non passa
        # la regola di qualita', riproviamo in automatico con questo modello
        # (piu' capace) prima di far scattare il breaker.
        self.fallback_model = os.environ.get("SPENDGUARD_FALLBACK", "")
        # Consensus Firewall (modalita' High-Stakes): due modelli indipendenti
        # devono concordare sull'azione prima che venga eseguita.
        self.require_consensus = False
        self.consensus_model = os.environ.get("SPENDGUARD_CONSENSUS", "")
        self.consensus_strictness = "exact"     # 'exact' | 'keys' | 'names'
        self.check_policy: dict | None = None
        self.trip_after = int(os.environ.get("SPENDGUARD_TRIP_AFTER", "2"))

    def public(self) -> dict[str, Any]:
        # Non restituiamo mai la chiave: solo se e' impostata.
        return {"upstream": self.upstream, "model": self.model,
                "fallback_model": self.fallback_model,
                "require_consensus": self.require_consensus,
                "consensus_model": self.consensus_model,
                "consensus_strictness": self.consensus_strictness,
                "has_key": bool(self.api_key), "trip_after": self.trip_after,
                "check_policy": self.check_policy}


config = Config()


class Budget:
    def __init__(self, limit_usd: float) -> None:
        self.limit = float(limit_usd)
        self.spent = 0.0
        self.calls = 0
        self.refused = 0
        self.halted = 0

    @property
    def remaining(self) -> float:
        return max(0.0, self.limit - self.spent)

    def as_dict(self) -> dict[str, Any]:
        return {"limit_usd": self.limit, "spent_usd": self.spent,
                "remaining_usd": self.remaining, "calls": self.calls,
                "refused": self.refused, "halted": self.halted}


budget = Budget(float(os.environ.get("SPENDGUARD_BUDGET", "0.05")))
_budget_lock = asyncio.Lock()
_demo_task: "asyncio.Task | None" = None


# =============================================================================
#  STATEFUL LOOP DETECTION — la terza linea di difesa
# =============================================================================
#  I proxy tradizionali sono senza memoria: vedono ogni richiesta isolata e non
#  possono accorgersi che un agente e' incastrato in un `while True` che rimanda
#  lo STESSO identico prompt. Qui teniamo una memoria leggera per identita'
#  (chiave API) e blocchiamo il loop PRIMA di inoltrare al provider: costo zero.
#
#  Note di progetto:
#   - L'identita' e' l'HASH della chiave, mai la chiave in chiaro.
#   - L'impronta del prompt e' SHA-256 di (modello + ruoli + contenuti).
#   - Il breaker si RIAPRE da solo se l'agente manda un prompt diverso: significa
#     che si e' sbloccato, e non vogliamo lasciarlo fuori per sempre.
#   - La memoria viene potata: nessuna crescita illimitata.

class LoopDetector:
    def __init__(self, threshold: int = 3, window_s: float = 60.0) -> None:
        self.enabled = True
        self.threshold = int(threshold)
        self.window_s = float(window_s)
        self._hist: dict[str, deque] = {}     # identita' -> deque[(impronta, ts)]
        self._state: dict[str, dict] = {}     # identita' -> {tripped, hash}
        self.trips = 0
        self.blocked = 0
        self._max_identities = 500

    # ---- utilita' -------------------------------------------------------
    @staticmethod
    def identity(auth_header: str | None) -> str:
        """Bucket per chiave API. Non teniamo mai la chiave in chiaro."""
        if not auth_header:
            return "local"
        return hashlib.sha256(auth_header.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def fingerprint(model: str, messages: list) -> str:
        h = hashlib.sha256()
        h.update((model or "").encode("utf-8"))
        for m in messages or []:
            h.update(b"\x1f")
            h.update(str(m.get("role", "")).encode("utf-8"))
            c = m.get("content", "")
            if not isinstance(c, str):
                c = json.dumps(c, sort_keys=True, ensure_ascii=False)
            h.update(b"\x1e")
            h.update(c.encode("utf-8"))
        return h.hexdigest()

    def _consecutive(self, dq: deque, fp: str, now: float) -> int:
        """Quante volte di fila (dalla piu' recente) compare `fp` nella finestra."""
        n = 0
        for h, ts in reversed(dq):
            if h != fp or (now - ts) > self.window_s:
                break
            n += 1
        return n

    def _prune(self, now: float) -> None:
        stale = [k for k, dq in self._hist.items()
                 if not dq or (now - dq[-1][1]) > self.window_s * 3]
        for k in stale:
            self._hist.pop(k, None)
            self._state.pop(k, None)
        if len(self._hist) > self._max_identities:      # difesa dalla crescita
            for k in list(self._hist)[:len(self._hist) - self._max_identities]:
                self._hist.pop(k, None)
                self._state.pop(k, None)

    # ---- il metodo che conta -------------------------------------------
    def observe(self, ident: str, fp: str) -> tuple[str, int]:
        """Registra la richiesta. Ritorna (verdetto, ripetizioni):
        'ok' | 'repeat' (avviso) | 'trip' (scatta ora) | 'block' (gia' scattato)."""
        now = time.monotonic()
        self._prune(now)
        st = self._state.setdefault(ident, {"tripped": False, "hash": None})
        dq = self._hist.setdefault(ident, deque(maxlen=64))

        if st["tripped"]:
            if fp != st["hash"]:
                # prompt diverso: l'agente si e' sbloccato -> richiudiamo
                st["tripped"] = False
                st["hash"] = None
                dq.clear()
            else:
                self.blocked += 1
                return ("block", max(self.threshold, self._consecutive(dq, fp, now)))

        dq.append((fp, now))
        n = self._consecutive(dq, fp, now)
        if n >= self.threshold:
            st["tripped"] = True
            st["hash"] = fp
            self.trips += 1
            self.blocked += 1
            return ("trip", n)
        return ("repeat" if n > 1 else "ok", n)

    def reset(self) -> None:
        self._hist.clear()
        self._state.clear()
        self.trips = 0
        self.blocked = 0

    def stats(self) -> dict:
        return {"loop_enabled": self.enabled, "loop_threshold": self.threshold,
                "loop_window_s": self.window_s, "loop_trips": self.trips,
                "loop_blocked": self.blocked}


loop_detector = LoopDetector(
    threshold=int(os.environ.get("SPENDGUARD_LOOP_THRESHOLD", "3")),
    window_s=float(os.environ.get("SPENDGUARD_LOOP_WINDOW", "60")))


# =============================================================================
#  TOOL-CALL ACTION FIREWALL — la protezione delle AZIONI
# =============================================================================
#  Budget e qualita' proteggono soldi e logica. Ma un agente allucinato che
#  emette `delete_database` fa un danno che nessun cap ripaga: i proxy
#  tradizionali inoltrano quel JSON al client, che lo esegue.
#  Qui ispezioniamo i tool_call IN USCITA e blocchiamo i nomi vietati PRIMA che
#  raggiungano l'esecutore. Il nome arriva nel primo chunk del tool_call, quindi
#  possiamo tagliare a meta' stream: azione bloccata e token risparmiati.
#
#  Due modalita', entrambe supportate:
#   - deny-list  (`blocked_tools`): tutto permesso tranne cio' che combacia.
#   - allow-list (`allowed_tools`): se valorizzata, TUTTO il resto e' vietato.
#     E' piu' forte (default-deny) ed e' quella da consigliare in produzione.
#
#  Pattern: glob (`delete_*`, `refund`) oppure regex con prefisso `re:`.

class ToolFirewall:
    def __init__(self) -> None:
        self.blocked_patterns: list[str] = []
        self.allowed_patterns: list[str] = []
        self.mode = "block"        # 'block' -> HTTP 403 | 'override' -> risposta controllata
        self.blocked_count = 0
        self._blocked_re: list = []
        self._allowed_re: list = []

    @staticmethod
    def _compile(patterns: list[str]) -> list:
        out = []
        for p in patterns:
            p = (p or "").strip()
            if not p:
                continue
            try:
                if p.lower().startswith("re:"):
                    out.append(re.compile(p[3:], re.IGNORECASE))
                else:
                    # glob -> regex (delete_* diventa ^delete_.*$), case-insensitive
                    out.append(re.compile(fnmatch.translate(p), re.IGNORECASE))
            except re.error:
                continue      # pattern malformato: ignorato, non deve rompere il proxy
        return out

    def configure(self, blocked=None, allowed=None, mode=None) -> None:
        if blocked is not None:
            self.blocked_patterns = [str(x) for x in blocked if str(x).strip()]
            self._blocked_re = self._compile(self.blocked_patterns)
        if allowed is not None:
            self.allowed_patterns = [str(x) for x in allowed if str(x).strip()]
            self._allowed_re = self._compile(self.allowed_patterns)
        if mode in ("block", "override"):
            self.mode = mode

    def active(self) -> bool:
        return bool(self._blocked_re or self._allowed_re)

    def verdict(self, name: str) -> tuple[bool, str]:
        """Ritorna (bloccato, motivo). Ispezione O(numero di pattern): leggera."""
        if not name:
            return (False, "")
        # allow-list: se c'e', tutto cio' che non combacia e' vietato (default-deny)
        if self._allowed_re and not any(r.match(name) for r in self._allowed_re):
            return (True, "not in allow-list")
        for r, p in zip(self._blocked_re, self.blocked_patterns):
            if r.match(name):
                return (True, "matches blocked pattern '" + p + "'")
        return (False, "")

    def stats(self) -> dict:
        return {"tools_firewall_active": self.active(),
                "blocked_tools": self.blocked_patterns,
                "allowed_tools": self.allowed_patterns,
                "tool_firewall_mode": self.mode,
                "tools_blocked_total": self.blocked_count}


tool_firewall = ToolFirewall()
_env_blocked = os.environ.get("SPENDGUARD_BLOCKED_TOOLS", "")
if _env_blocked:
    tool_firewall.configure(blocked=[p for p in _env_blocked.split(",")])


# =============================================================================
#  CONSENSUS FIREWALL — sistema a doppia chiave per le azioni critiche
# =============================================================================
#  Un solo modello puo' allucinare un'azione distruttiva con totale sicurezza.
#  In modalita' High-Stakes la stessa richiesta va a DUE modelli indipendenti in
#  PARALLELO (asyncio.gather -> la latenza e' quella del piu' lento, non la
#  somma). L'azione viene eseguita solo se entrambi scelgono lo stesso strumento
#  con gli stessi parametri. Se discordano, non si esegue: e' il principio delle
#  due chiavi per lanciare il missile.
#
#  Fail-closed: se il modello di controllo non risponde, l'azione NON passa.
#  Per una funzione di sicurezza e' l'unica scelta difendibile.

def _canon_args(raw: str):
    """Normalizza gli argomenti di un tool call per il confronto."""
    try:
        return json.loads(raw or "{}")
    except Exception:
        return {"__raw__": (raw or "").strip()}


def _named_tools(tool_calls: list) -> list[tuple[str, Any]]:
    out = []
    for t in tool_calls or []:
        fn = t.get("function") or {}
        out.append((fn.get("name", ""), _canon_args(fn.get("arguments", ""))))
    return sorted(out, key=lambda x: x[0])


def consensus_verdict(primary_tools: list, second_tools: list,
                      strictness: str = "exact") -> tuple[bool, str]:
    """Ritorna (concordi, motivo). Nessuna azione da entrambe le parti = concordi."""
    a, b = _named_tools(primary_tools), _named_tools(second_tools)

    if not a and not b:
        return (True, "no action requested by either model")
    if bool(a) != bool(b):
        who = "primary" if a else "secondary"
        names = [n for n, _ in (a or b)]
        return (False, "only the " + who + " model wanted to act (" +
                ", ".join(names) + ")")

    names_a = [n for n, _ in a]
    names_b = [n for n, _ in b]
    if names_a != names_b:
        return (False, "different tools chosen: " + str(names_a) + " vs " + str(names_b))
    if strictness == "names":
        return (True, "tool names match")

    for (na, aa), (nb, ab) in zip(a, b):
        if strictness == "keys":
            shared = set(aa) & set(ab)
            diff = [k for k in shared if aa[k] != ab[k]]
            if diff:
                return (False, "tool '" + na + "': parameters differ on " + str(diff))
        else:   # 'exact'
            if aa != ab:
                return (False, "tool '" + na + "': arguments differ ("
                        + json.dumps(aa, sort_keys=True)[:80] + " vs "
                        + json.dumps(ab, sort_keys=True)[:80] + ")")
    return (True, "both models chose the same action with the same parameters")


# =============================================================================
#  SHADOW MODE — la modalita' che rende adottabile un firewall bloccante
# =============================================================================
#  Nessuno mette in produzione qualcosa che blocca senza prima sapere QUANTE
#  chiamate valide bloccherebbe. In shadow, SpendGuard valuta tutte le difese
#  comportamentali ma lascia passare, registrando cosa AVREBBE bloccato e quanto
#  AVREBBE risparmiato — il risparmio e' il costo reale delle chiamate che sono
#  passate solo perche' eravamo in shadow: una misura esatta, non una stima.
#
#  Nota di sicurezza: il tetto di spesa resta SEMPRE attivo anche in shadow.
#  Simulare il budget significherebbe spendere davvero soldi non autorizzati.

class Enforcement:
    def __init__(self) -> None:
        self.mode = "enforce"                 # 'enforce' | 'shadow'
        self.incidents: deque = deque(maxlen=200)
        self.would_block = 0
        self.saved_usd = 0.0

    def shadow(self) -> bool:
        return self.mode == "shadow"

    def record(self, kind: str, reason: str, model: str = "") -> None:
        self.would_block += 1
        self.incidents.appendleft({"t": time.time(), "kind": kind,
                                   "reason": reason, "model": model})

    def add_saved(self, usd: float) -> None:
        self.saved_usd += max(0.0, float(usd or 0.0))

    def reset(self) -> None:
        self.incidents.clear()
        self.would_block = 0
        self.saved_usd = 0.0

    def stats(self) -> dict:
        return {"enforcement_mode": self.mode,
                "shadow_would_block": self.would_block,
                "shadow_saved_usd": round(self.saved_usd, 6)}


enforcement = Enforcement()

_prices = dict(DEFAULT_PRICES_PER_1M)
_prices["runaway-reasoner"] = (2.00, 6.00)   # modello del provider finto (demo)

_breakers: dict[str, dict] = {}
_fallback_attempts = 0
_fallback_saves = 0


def _breaker(route: str) -> dict:
    return _breakers.setdefault(route, {"fails": 0, "tripped": False,
                                        "total_fail": 0, "total_pass": 0})


def _quality_stats() -> dict:
    return {"quality_enabled": config.check_policy is not None,
            "quality_pass_total": sum(b["total_pass"] for b in _breakers.values()),
            "quality_fail_total": sum(b["total_fail"] for b in _breakers.values()),
            "quality_tripped": any(b["tripped"] for b in _breakers.values()),
            "fallback_attempts": _fallback_attempts,
            "fallback_saves": _fallback_saves,
            "trip_after": config.trip_after}


def _price(model: str) -> tuple[float, float]:
    in_1m, out_1m = _prices.get(model, FALLBACK_PRICE_PER_1M)
    return in_1m / 1_000_000.0, out_1m / 1_000_000.0


def _messages_to_text(messages: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for m in messages:
        c = m.get("content", "")
        if isinstance(c, str):
            parts.append(c)
        elif isinstance(c, list):
            for chunk in c:
                if isinstance(chunk, dict) and chunk.get("type") == "text":
                    parts.append(chunk.get("text", ""))
    return "\n".join(parts)


def _now() -> int:
    return int(time.time())


# =============================================================================
#  Eventi verso la dashboard
# =============================================================================

_subscribers: set[asyncio.Queue] = set()


async def publish(kind: str, **detail: Any) -> None:
    ev = {"kind": kind, "t": time.time(), **detail}
    for q in list(_subscribers):
        try:
            q.put_nowait(ev)
        except Exception:
            pass


def _reset_quality_counters() -> None:
    """Azzera breaker e contatori del fallback (usato da reset/config/demo)."""
    global _fallback_attempts, _fallback_saves
    _breakers.clear()
    _fallback_attempts = 0
    _fallback_saves = 0
    loop_detector.reset()
    tool_firewall.blocked_count = 0
    enforcement.reset()


def _validate(content: str):
    """Valida l'output secondo la policy attiva. Ritorna (ok, motivo)."""
    if config.check_policy is None:
        return (True, "")
    return run_check(content, config.check_policy)


async def _record_quality(model: str, ok: bool, reason: str = "",
                          rescued_by: str | None = None) -> bool:
    """Aggiorna il breaker della route. Ritorna True se e' appena scattato.
    `rescued_by` = la chiamata e' stata salvata dal fallback: conta come pass."""
    b = _breaker(model)
    if ok:
        b["fails"] = 0
        b["total_pass"] += 1
        await publish("quality_pass", model=model, rescued_by=rescued_by,
                      **_quality_stats())
        return False
    b["fails"] += 1
    b["total_fail"] += 1
    trip_now = b["fails"] >= config.trip_after and not b["tripped"]
    if trip_now:
        b["tripped"] = True
    await publish("quality_fail", model=model, reason=reason,
                  fails=b["fails"], **_quality_stats())
    if trip_now:
        await publish("quality_trip", model=model, reason=reason, **_quality_stats())
    return trip_now


# =============================================================================
#  Chiamata upstream con metering + kill-switch (concurrency-safe)
# =============================================================================

async def _metered_upstream(body, model, input_cost, price_out, hard_cap, auth_header=None):
    upstream_body = dict(body)
    upstream_body["stream"] = True
    upstream_body["max_tokens"] = hard_cap
    upstream_body["stream_options"] = {"include_usage": True}
    url = config.upstream.rstrip("/") + "/chat/completions"

    headers = {"content-type": "application/json"}
    key = None
    if auth_header:
        headers["authorization"] = auth_header      # chiave inviata dall'agente
    elif config.api_key:
        key = config.api_key                        # chiave impostata dalla dashboard
        headers["authorization"] = "Bearer " + key
    # Anthropic vuole anche i suoi header specifici
    if "anthropic.com" in config.upstream and (key or auth_header):
        raw = key or (auth_header or "").replace("Bearer ", "")
        headers["x-api-key"] = raw
        headers["anthropic-version"] = "2023-06-01"

    out_tokens, output_cost, since_progress = 0, 0.0, 0
    collected, halted, finish = [], False, None
    tool_slots: dict = {}          # index -> tool_call in costruzione
    blocked_tool = None            # (nome, motivo) se il firewall interviene

    async with _budget_lock:
        budget.spent += input_cost

    await publish("call_start", model=model, input_cost_usd=input_cost,
                  output_cap=hard_cap, **budget.as_dict())

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            async with client.stream("POST", url, json=upstream_body, headers=headers) as resp:
                if resp.status_code >= 400:
                    raw = (await resp.aread()).decode("utf-8", "replace")
                    detail = raw[:400]
                    try:
                        j = json.loads(raw)
                        detail = (j.get("error") or {}).get("message", detail)
                    except Exception:
                        pass
                    async with _budget_lock:
                        budget.spent -= input_cost      # nulla e' stato generato
                        budget.calls += 1
                    await publish("upstream_error", model=model,
                                  status=resp.status_code, detail=detail,
                                  **budget.as_dict())
                    yield {"type": "error", "status": resp.status_code, "detail": detail}
                    return
                async for line in resp.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue
                    data = line[len("data:"):].strip()
                    if data == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                    except Exception:
                        continue
                    choices = chunk.get("choices") or []
                    if choices:
                        delta = choices[0].get("delta") or {}

                        # ---- TOOL CALLS: passthrough + ACTION FIREWALL ----
                        tcs = delta.get("tool_calls")
                        if tcs:
                            for tc in tcs:
                                idx = tc.get("index", 0)
                                slot = tool_slots.setdefault(idx, {
                                    "id": None, "type": "function",
                                    "function": {"name": "", "arguments": ""}})
                                if tc.get("id"):
                                    slot["id"] = tc["id"]
                                if tc.get("type"):
                                    slot["type"] = tc["type"]
                                fn = tc.get("function") or {}
                                if fn.get("name"):
                                    slot["function"]["name"] += fn["name"]
                                    # ISPEZIONE: appena il nome e' disponibile.
                                    bad, why = tool_firewall.verdict(slot["function"]["name"])
                                    if bad:
                                        blocked_tool = (slot["function"]["name"], why)
                                        break
                                if fn.get("arguments"):
                                    arg = fn["arguments"]
                                    slot["function"]["arguments"] += arg
                                    # gli argomenti costano token: li contiamo
                                    dt = _count_tokens(arg, model)
                                    async with _budget_lock:
                                        budget.spent += dt * price_out
                                        output_cost += dt * price_out
                                        out_tokens += dt
                            if blocked_tool:
                                # taglio lo stream: l'azione non arrivera' mai al client
                                break
                            yield {"type": "tool_delta", "tool_calls": tcs}

                        piece = delta.get("content")
                        if piece:
                            dt = _count_tokens(piece, model)
                            dt_cost = dt * price_out
                            async with _budget_lock:
                                if budget.spent + dt_cost >= budget.limit:
                                    halted = True
                                else:
                                    budget.spent += dt_cost
                                    output_cost += dt_cost
                                    out_tokens += dt
                            if halted:
                                break
                            since_progress += dt
                            collected.append(piece)
                            yield {"type": "delta", "content": piece}
                            if since_progress >= PROGRESS_EVERY_TOKENS:
                                since_progress = 0
                                await publish("progress", model=model, output_tokens=out_tokens,
                                              live_cost_usd=input_cost + output_cost,
                                              projected_spent_usd=budget.spent,
                                              limit_usd=budget.limit)
                        fr = choices[0].get("finish_reason")
                        if fr:
                            finish = fr
    except Exception as exc:
        async with _budget_lock:
            if out_tokens == 0:
                budget.spent -= input_cost   # nulla generato: non deve costare
            budget.calls += 1
        await publish("upstream_error", model=model, status=502, detail=str(exc)[:300],
                      **budget.as_dict())
        yield {"type": "error", "status": 502, "detail": "upstream error: " + str(exc)}
        return

    call_cost = input_cost + output_cost
    async with _budget_lock:
        budget.calls += 1
        if halted:
            budget.halted += 1

    # Controllo finale di sicurezza: se un nome si fosse completato solo alla
    # fine (frammentato), lo intercettiamo comunque prima di consegnare.
    tools_out = [tool_slots[i] for i in sorted(tool_slots)] if tool_slots else []
    if blocked_tool is None:
        for t in tools_out:
            bad, why = tool_firewall.verdict(t["function"]["name"])
            if bad:
                blocked_tool = (t["function"]["name"], why)
                break

    if blocked_tool:
        tool_firewall.blocked_count += 1
        await publish("tool_blocked", model=model, tool=blocked_tool[0],
                      reason=blocked_tool[1], **budget.as_dict(),
                      **tool_firewall.stats())

    if halted:
        await publish("halt", model=model, output_tokens=out_tokens, **budget.as_dict())
    await publish("call_done", model=model, cost_usd=call_cost, output_tokens=out_tokens,
                  was_halted=halted, **budget.as_dict())
    yield {"type": "final", "cost_usd": call_cost, "output_tokens": out_tokens,
           "halted": halted, "finish": finish, "content": "".join(collected),
           "tool_calls": tools_out, "blocked_tool": blocked_tool}


# =============================================================================
#  Endpoint OpenAI-compatible enforced
# =============================================================================

async def _preflight(body: dict, model: str):
    """Controlli di budget per UNO specifico modello (i prezzi di primario e
    fallback sono diversi). Ritorna (refuse|None, input_tokens, input_cost,
    price_out, hard_cap)."""
    price_in, price_out = _price(model)
    text = _messages_to_text(body.get("messages", []))
    input_tokens = _count_tokens(text, model)
    input_cost = input_tokens * price_in

    refuse = None
    async with _budget_lock:
        remaining = budget.remaining
        if input_cost > remaining:
            refuse = "input_exceeds_budget"
        elif price_out > 0 and (remaining - input_cost) < price_out:
            refuse = "no_budget_for_output"
        if refuse:
            budget.refused += 1
    if refuse:
        return (refuse, input_tokens, input_cost, price_out, 0)

    affordable = int((budget.remaining - input_cost) // price_out) if price_out > 0 else 10 ** 7
    requested = body.get("max_tokens") or DEFAULT_MAX_TOKENS
    # non gonfiamo mai la richiesta del client: al massimo la riduciamo
    hard_cap = max(1, min(requested, affordable))
    return (None, input_tokens, input_cost, price_out, hard_cap)


async def _attempt(body: dict, model: str, auth_header):
    """Una chiamata completa, BUFFERIZZATA, verso `model`. Niente streaming al
    client: cosi' possiamo validare (ed eventualmente sostituire con il
    fallback) prima di rispondere."""
    call_body = dict(body)
    call_body["model"] = model
    refuse, input_tokens, input_cost, price_out, hard_cap = await _preflight(call_body, model)
    if refuse:
        await publish("refused", model=model, reason=refuse, needed_usd=input_cost,
                      **budget.as_dict())
        msg = ("input alone exceeds the remaining budget" if refuse == "input_exceeds_budget"
               else "no budget left to generate a reply")
        return {"status": 402, "content": "", "cost": 0.0, "halted": False,
                "finish": None, "input_tokens": input_tokens, "output_tokens": 0,
                "tool_calls": [], "blocked_tool": None,
                "error": "SpendGuard refused before sending: " + msg + "."}

    final = None
    async for item in _metered_upstream(call_body, model, input_cost, price_out,
                                        hard_cap, auth_header):
        if item["type"] == "error":
            return {"status": item.get("status", 502), "content": "", "cost": 0.0,
                    "halted": False, "finish": None, "input_tokens": input_tokens,
                    "output_tokens": 0, "tool_calls": [], "blocked_tool": None,
                    "error": item["detail"]}
        if item["type"] == "final":
            final = item
    return {"status": 200, "content": final["content"], "cost": final["cost_usd"],
            "halted": final["halted"], "finish": final["finish"],
            "input_tokens": input_tokens, "output_tokens": final["output_tokens"],
            "tool_calls": final.get("tool_calls") or [],
            "blocked_tool": final.get("blocked_tool"), "error": None}


def _assistant_message(result) -> dict:
    msg = {"role": "assistant", "content": result["content"] or None}
    if result.get("tool_calls"):
        msg["tool_calls"] = result["tool_calls"]
    return msg


def _completion_json(model, result, reason, extra=None):
    payload = {
        "id": "chatcmpl-spendguard", "object": "chat.completion", "created": _now(),
        "model": model,
        "choices": [{"index": 0,
                     "message": _assistant_message(result),
                     "finish_reason": reason}],
        "usage": {"prompt_tokens": result["input_tokens"],
                  "completion_tokens": result["output_tokens"],
                  "total_tokens": result["input_tokens"] + result["output_tokens"]},
        "x_spendguard": {"cost_usd": result["cost"], "halted": result["halted"]},
    }
    if extra:
        payload["x_spendguard"].update(extra)
    return payload


def _as_sse(model, result, reason, extra=None):
    """Rende un risultato bufferizzato come stream SSE valido per il client."""
    def gen():
        if result["content"]:
            ch = {"id": "chatcmpl-spendguard", "object": "chat.completion.chunk",
                  "created": _now(), "model": model,
                  "choices": [{"index": 0, "delta": {"content": result["content"]},
                               "finish_reason": None}]}
            yield "data: " + json.dumps(ch) + "\n\n"
        if result.get("tool_calls"):
            ch = {"id": "chatcmpl-spendguard", "object": "chat.completion.chunk",
                  "created": _now(), "model": model,
                  "choices": [{"index": 0,
                               "delta": {"tool_calls": result["tool_calls"]},
                               "finish_reason": None}]}
            yield "data: " + json.dumps(ch) + "\n\n"
        fc = {"id": "chatcmpl-spendguard", "object": "chat.completion.chunk",
              "created": _now(), "model": model,
              "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]}
        if extra:
            fc["x_spendguard"] = extra
        yield "data: " + json.dumps(fc) + "\n\n"
        yield "data: [DONE]\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    global _fallback_attempts, _fallback_saves
    body = await request.json()
    model = body.get("model") or config.model
    body["model"] = model
    auth_header = request.headers.get("authorization")
    quality_on = config.check_policy is not None
    want_stream = bool(body.get("stream"))

    # --- DIFESA 1: STATEFUL LOOP DETECTION -------------------------------
    # Costa zero e non tocca il provider: va per prima. Se l'agente ripete lo
    # stesso identico prompt, il loop viene spezzato qui, a spesa nulla.
    shadow_hit = None      # ('kind', 'motivo') se in shadow avremmo bloccato
    if loop_detector.enabled:
        ident = LoopDetector.identity(auth_header)
        fp = LoopDetector.fingerprint(model, body.get("messages", []))
        verdict, n = loop_detector.observe(ident, fp)
        if verdict in ("trip", "block"):
            msg = ("Agent Loop Detected: Identical prompt submitted " + str(n)
                   + " times. Circuit broken to prevent budget burn.")
            if enforcement.shadow():
                shadow_hit = ("agent_loop", msg)
                enforcement.record("agent_loop", msg, model)
                await publish("shadow_block", defense="agent_loop", reason=msg,
                              model=model, **enforcement.stats())
            else:
                await publish("loop_detected" if verdict == "trip" else "loop_blocked",
                              model=model, count=n, **budget.as_dict(),
                              **_quality_stats(), **loop_detector.stats())
                return JSONResponse(status_code=429, content={
                    "error": {"type": "agent_loop_detected",
                              "code": "agent_loop_detected", "message": msg},
                    "x_spendguard": {"identical_prompts": n,
                                     "window_seconds": loop_detector.window_s,
                                     "threshold": loop_detector.threshold,
                                     "hint": "Send a different prompt to close the breaker."}})
        elif verdict == "repeat":
            await publish("loop_repeat", model=model, count=n,
                          **loop_detector.stats())

    # --- DIFESA 2: breaker di qualita' gia' aperto ------------------------
    if quality_on and _breaker(model)["tripped"]:
        qmsg = ("SpendGuard quality breaker is OPEN for '" + model + "': "
                + str(config.trip_after) + " consecutive validation failures.")
        if enforcement.shadow():
            shadow_hit = shadow_hit or ("quality_breaker", qmsg)
            enforcement.record("quality_breaker", qmsg, model)
            await publish("shadow_block", defense="quality_breaker", reason=qmsg,
                          model=model, **enforcement.stats())
        else:
            await publish("quality_refused", model=model, **_quality_stats())
            return JSONResponse(status_code=409, content={"error": {
                "type": "quality_circuit_open", "code": "quality_circuit_open",
                "message": qmsg}})

    # -----------------------------------------------------------------
    # Nessuna regola di qualita' + streaming -> passthrough puro (latenza minima).
    # Se pero' la richiesta dichiara `tools` e il firewall e' attivo, passiamo in
    # bufferizzato: e' l'unico modo per restituire un 403 pulito invece di aver
    # gia' consegnato al client un'azione distruttiva.
    has_tools = bool(body.get("tools") or body.get("functions"))
    consensus_on = bool(config.require_consensus and config.consensus_model
                        and config.consensus_model != model and has_tools)
    needs_buffer = quality_on or ((tool_firewall.active() or consensus_on) and has_tools)
    if not needs_buffer and want_stream:
        refuse, input_tokens, input_cost, price_out, hard_cap = await _preflight(body, model)
        if refuse:
            await publish("refused", model=model, reason=refuse, needed_usd=input_cost,
                          **budget.as_dict())
            msg = ("input alone exceeds the remaining budget"
                   if refuse == "input_exceeds_budget" else "no budget left to generate a reply")
            return JSONResponse(status_code=402, content={"error": {
                "type": "budget_exceeded", "code": "insufficient_budget",
                "message": "SpendGuard refused before sending: " + msg + "."}})

        async def sse():
            async for item in _metered_upstream(body, model, input_cost, price_out,
                                                hard_cap, auth_header):
                if item["type"] == "delta":
                    ch = {"id": "chatcmpl-spendguard", "object": "chat.completion.chunk",
                          "created": _now(), "model": model,
                          "choices": [{"index": 0, "delta": {"content": item["content"]},
                                       "finish_reason": None}]}
                    yield "data: " + json.dumps(ch) + "\n\n"
                elif item["type"] == "tool_delta":
                    ch = {"id": "chatcmpl-spendguard", "object": "chat.completion.chunk",
                          "created": _now(), "model": model,
                          "choices": [{"index": 0,
                                       "delta": {"tool_calls": item["tool_calls"]},
                                       "finish_reason": None}]}
                    yield "data: " + json.dumps(ch) + "\n\n"
                elif item["type"] == "error":
                    yield "data: " + json.dumps({"error": {"type": "upstream_error",
                                                           "message": item["detail"]}}) + "\n\n"
                    yield "data: [DONE]\n\n"
                else:
                    if item.get("blocked_tool"):
                        # Non possiamo piu' cambiare lo status HTTP (gia' 200):
                        # segnaliamo il blocco nello stream e chiudiamo.
                        name, why = item["blocked_tool"]
                        yield "data: " + json.dumps({"error": {
                            "type": "tool_call_blocked", "code": "tool_call_blocked",
                            "message": ("SpendGuard Action Firewall blocked tool '"
                                        + name + "' (" + why + "). The call was not "
                                        "delivered to your client.")}}) + "\n\n"
                        yield "data: [DONE]\n\n"
                        return
                    reason = "budget_halt" if item["halted"] else (item["finish"] or "stop")
                    fc = {"id": "chatcmpl-spendguard", "object": "chat.completion.chunk",
                          "created": _now(), "model": model,
                          "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]}
                    yield "data: " + json.dumps(fc) + "\n\n"
                    yield "data: [DONE]\n\n"
        return StreamingResponse(sse(), media_type="text/event-stream")

    # -----------------------------------------------------------------
    # Modalita' bufferizzata: valida e, se serve, QUALITY-TRIGGERED FALLBACK.
    # (obbligatoria con la qualita' attiva: i token gia' inviati non si ritirano)
    # -----------------------------------------------------------------
    primary = await _attempt(body, model, auth_header) if not consensus_on else None
    second = None
    if consensus_on:
        # DIFESA 5: CONSENSUS FIREWALL — le due chiamate partono INSIEME, quindi
        # la latenza e' quella del piu' lento, non la somma.
        await publish("consensus_start", model=model,
                      consensus_model=config.consensus_model)
        primary, second = await asyncio.gather(
            _attempt(body, model, auth_header),
            _attempt(body, config.consensus_model, auth_header))

    if primary["status"] != 200:
        err_type = "budget_exceeded" if primary["status"] == 402 else "upstream_error"
        code = "insufficient_budget" if primary["status"] == 402 else "upstream_error"
        return JSONResponse(status_code=primary["status"], content={"error": {
            "type": err_type, "code": code, "message": primary["error"]}})

    # --- DIFESA 4: TOOL-CALL ACTION FIREWALL -----------------------------
    # L'LLM ha chiesto un'azione vietata: la risposta pericolosa viene scartata
    # e non raggiunge MAI l'esecutore del cliente.
    if primary.get("blocked_tool"):
        name, why = primary["blocked_tool"]
        if enforcement.shadow():
            fmsg = "tool '" + name + "' would be blocked (" + why + ")"
            shadow_hit = shadow_hit or ("tool_call", fmsg)
            enforcement.record("tool_call", fmsg, model)
            await publish("shadow_block", defense="tool_call", reason=fmsg,
                          model=model, **enforcement.stats())
            primary["blocked_tool"] = None      # in shadow lasciamo passare
        elif tool_firewall.mode == "override":
            # Override controllato: l'agente riceve un 200 con una spiegazione,
            # cosi' puo' correggersi invece di andare in crash su un 403.
            note = ("[SpendGuard] The tool '" + name + "' is blocked by policy ("
                    + why + ") and was NOT executed. Choose a different approach "
                    "or ask a human to perform this action.")
            overridden = dict(primary)
            overridden["content"] = note
            overridden["tool_calls"] = []
            extra = {"tool_call_blocked": True, "blocked_tool": name, "reason": why,
                     "mode": "override"}
            return (_as_sse(model, overridden, "stop", extra) if want_stream
                    else JSONResponse(content=_completion_json(model, overridden,
                                                               "stop", extra)))
        return JSONResponse(status_code=403, content={
            "error": {"type": "tool_call_blocked", "code": "tool_call_blocked",
                      "message": ("SpendGuard Action Firewall blocked tool '" + name
                                  + "' (" + why + "). The destructive call was "
                                  "discarded and never reached your executor.")},
            "x_spendguard": {"blocked_tool": name, "reason": why,
                             "cost_usd": primary["cost"],
                             "hint": "Adjust blocked_tools/allowed_tools to change this."}})

    # --- DIFESA 5: CONSENSUS FIREWALL (verdetto) --------------------------
    if consensus_on:
        combined_cost = primary["cost"] + (second["cost"] if second else 0.0)
        if second is None or second["status"] != 200:
            # fail-closed: senza secondo parere non si esegue nulla.
            reason = ("secondary model unavailable: "
                      + str((second or {}).get("error") or "no response"))
            agreed = False
        elif second.get("blocked_tool"):
            reason = ("secondary model requested a forbidden tool: "
                      + second["blocked_tool"][0])
            agreed = False
        else:
            agreed, reason = consensus_verdict(primary.get("tool_calls") or [],
                                               second.get("tool_calls") or [],
                                               config.consensus_strictness)
        if agreed:
            await publish("consensus_ok", model=model,
                          consensus_model=config.consensus_model, reason=reason,
                          cost_usd=combined_cost, **budget.as_dict())
            extra = {"consensus": "passed", "consensus_model": config.consensus_model,
                     "consensus_reason": reason, "combined_cost_usd": combined_cost}
            fr = primary["finish"] or "stop"
            return (_as_sse(model, primary, fr, extra) if want_stream
                    else JSONResponse(content=_completion_json(model, primary, fr, extra)))

        cmsg = ("Consensus Failed: Primary and Secondary models disagreed on the "
                "execution path. (" + reason + ")")
        if enforcement.shadow():
            shadow_hit = shadow_hit or ("consensus", cmsg)
            enforcement.record("consensus", cmsg, model)
            await publish("shadow_block", defense="consensus", reason=cmsg,
                          model=model, **enforcement.stats())
            enforcement.add_saved(combined_cost)
            extra = {"consensus": "failed_shadow", "consensus_reason": reason}
            fr = primary["finish"] or "stop"
            return (_as_sse(model, primary, fr, extra) if want_stream
                    else JSONResponse(content=_completion_json(model, primary, fr, extra)))

        await publish("consensus_failed", model=model,
                      consensus_model=config.consensus_model, reason=reason,
                      cost_usd=combined_cost, **budget.as_dict())
        return JSONResponse(status_code=403, content={
            "error": {"type": "consensus_failed", "code": "consensus_failed",
                      "message": cmsg},
            "x_spendguard": {
                "primary_model": model, "secondary_model": config.consensus_model,
                "primary_action": [t.get("function", {}).get("name")
                                   for t in (primary.get("tool_calls") or [])],
                "secondary_action": [t.get("function", {}).get("name")
                                     for t in ((second or {}).get("tool_calls") or [])],
                "disagreement": reason, "combined_cost_usd": combined_cost,
                "strictness": config.consensus_strictness}})

    # Output tagliato dal budget o qualita' spenta: nessun giudizio di merito.
    if primary["halted"] or not quality_on:
        if shadow_hit:
            enforcement.add_saved(primary["cost"])
        reason = "budget_halt" if primary["halted"] else (primary["finish"] or "stop")
        return (_as_sse(model, primary, reason) if want_stream
                else JSONResponse(content=_completion_json(model, primary, reason)))

    if shadow_hit:
        enforcement.add_saved(primary["cost"])

    ok, why = _validate(primary["content"])
    if ok:
        await _record_quality(model, True)
        reason = primary["finish"] or "stop"
        return (_as_sse(model, primary, reason) if want_stream
                else JSONResponse(content=_completion_json(model, primary, reason)))

    # ---- QUALITY-TRIGGERED FALLBACK ---------------------------------
    fb = (config.fallback_model or "").strip()
    if fb and fb != model:
        _fallback_attempts += 1
        await publish("fallback_attempt", model=model, fallback_model=fb,
                      reason=why, **_quality_stats())
        second = await _attempt(body, fb, auth_header)
        if second["status"] == 200 and not second["halted"]:
            ok2, why2 = _validate(second["content"])
            if ok2:
                # Il fallback ha salvato la chiamata: il client riceve 200 OK.
                _fallback_saves += 1
                _breaker(model)["fails"] = 0     # risolta: il breaker non scatta
                await _record_quality(model, True, rescued_by=fb)
                await publish("fallback_success", model=model, fallback_model=fb,
                              cost_usd=second["cost"], **_quality_stats())
                reason = second["finish"] or "stop"
                extra = {"fallback_used": True, "primary_model": model,
                         "fallback_model": fb, "primary_failure": why}
                return (_as_sse(fb, second, reason, extra) if want_stream
                        else JSONResponse(content=_completion_json(fb, second, reason, extra)))
            why = "primary: " + why + " | fallback: " + why2
        else:
            why = ("primary: " + why + " | fallback unavailable: "
                   + str(second.get("error") or "halted by budget"))
        await publish("fallback_failed", model=model, fallback_model=fb,
                      reason=why, **_quality_stats())

    # ---- nessun fallback, o fallito anche lui: conta il fallimento ----
    tripped_now = await _record_quality(model, False, why)
    if tripped_now:
        # Appena scattato: non restituiamo output sbagliato, rispondiamo 409.
        return JSONResponse(status_code=409, content={"error": {
            "type": "quality_circuit_open", "code": "quality_circuit_open",
            "message": ("SpendGuard quality breaker just OPENED for '" + model
                        + "': " + str(config.trip_after)
                        + " consecutive validation failures. Last reason: " + why)}})

    reason = primary["finish"] or "stop"
    extra = {"quality_failed": True, "quality_reason": why}
    return (_as_sse(model, primary, reason, extra) if want_stream
            else JSONResponse(content=_completion_json(model, primary, reason, extra)))


# =============================================================================
#  Config / stats / eventi — tutto pilotabile dalla dashboard
# =============================================================================

@app.get("/stats")
async def stats():
    d = budget.as_dict()
    d.update(_quality_stats())
    d.update(loop_detector.stats())
    d.update(tool_firewall.stats())
    d.update(enforcement.stats())
    d.update(config.public())
    return d


@app.get("/config")
async def get_config():
    return config.public()


@app.post("/config")
async def set_config(request: Request):
    """Imposta provider, chiave, modello, budget e regola di qualita'.
    Ogni campo e' opzionale: si aggiorna solo quello che arriva."""
    data = await request.json()
    await _stop_demo()

    if data.get("provider") in PROVIDER_PRESETS:
        config.upstream = PROVIDER_PRESETS[data["provider"]]
    if data.get("upstream"):
        config.upstream = str(data["upstream"]).strip()
    if data.get("api_key") is not None:
        config.api_key = str(data["api_key"]).strip()   # solo in memoria
    if data.get("model"):
        config.model = str(data["model"]).strip()
    if "fallback_model" in data:
        config.fallback_model = str(data.get("fallback_model") or "").strip()
    if data.get("require_consensus") is not None:
        config.require_consensus = bool(data["require_consensus"])
    if "consensus_model" in data:
        config.consensus_model = str(data.get("consensus_model") or "").strip()
    if data.get("consensus_strictness") in ("exact", "keys", "names"):
        config.consensus_strictness = data["consensus_strictness"]
    if data.get("enforcement_mode") in ("enforce", "shadow"):
        enforcement.mode = data["enforcement_mode"]
    if "blocked_tools" in data or "allowed_tools" in data or "tool_firewall_mode" in data:
        def _as_list(v):
            if v is None:
                return None
            if isinstance(v, str):
                return [x.strip() for x in v.split(",") if x.strip()]
            return list(v)
        tool_firewall.configure(blocked=_as_list(data.get("blocked_tools")),
                                allowed=_as_list(data.get("allowed_tools")),
                                mode=data.get("tool_firewall_mode"))
    if data.get("loop_detection") is not None:
        loop_detector.enabled = bool(data["loop_detection"])
    if data.get("loop_threshold") is not None:
        try:
            loop_detector.threshold = max(2, int(data["loop_threshold"]))
        except Exception:
            pass
    if data.get("loop_window_s") is not None:
        try:
            loop_detector.window_s = max(1.0, float(data["loop_window_s"]))
        except Exception:
            pass
    if data.get("trip_after") is not None:
        try:
            config.trip_after = max(1, int(data["trip_after"]))
        except Exception:
            pass
    if "check_policy" in data:
        config.check_policy = data["check_policy"] or None

    if data.get("budget_usd") is not None:
        try:
            v = float(data["budget_usd"])
            if v <= 0:
                raise ValueError
        except Exception:
            return JSONResponse(status_code=400,
                                content={"error": "budget_usd must be a positive number"})
        async with _budget_lock:
            budget.limit = v

    if data.get("reset", True):
        _reset_quality_counters()
        async with _budget_lock:
            budget.spent = 0.0
            budget.calls = budget.refused = budget.halted = 0

    await publish("reset", **budget.as_dict(), **_quality_stats())
    return {"ok": True, **config.public(), **budget.as_dict()}


@app.get("/incidents")
async def incidents():
    """Referto dello shadow mode: cosa avremmo bloccato e quanto avremmo risparmiato."""
    return {"incidents": list(enforcement.incidents), **enforcement.stats()}


@app.get("/events")
async def events():
    q: asyncio.Queue = asyncio.Queue()
    _subscribers.add(q)

    async def gen():
        yield "data: " + json.dumps({"kind": "stats", **budget.as_dict(),
                                     **_quality_stats(), **loop_detector.stats(),
                                     **tool_firewall.stats(), **enforcement.stats(),
                                     **config.public()}) + "\n\n"
        try:
            while True:
                ev = await q.get()
                yield "data: " + json.dumps(ev) + "\n\n"
        finally:
            _subscribers.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


# =============================================================================
#  Agenti di prova (girano dal browser: nessun terminale)
# =============================================================================

async def _loop_agent(user_msg: str, system_msg: str, model: str, max_iter: int = 80,
                      identical: bool = False) -> None:
    """Martella il proxy in loop finche' non viene fermato
    (402 budget / 409 qualita' / 429 loop).

    `identical=False` (default): il prompt varia ad ogni giro, come un agente che
    accumula contesto -> mette alla prova budget e qualita'.
    `identical=True`: manda lo STESSO identico prompt -> mette alla prova il
    rilevatore di loop comportamentale."""
    url = "http://127.0.0.1:" + str(PROXY_PORT) + "/v1/chat/completions"
    try:
        async with httpx.AsyncClient(timeout=90.0) as client:
            for i in range(max_iter):
                msg = user_msg if identical else (user_msg + " (step " + str(i + 1) + ")")
                body = {"model": model, "stream": True,
                        "messages": [{"role": "system", "content": system_msg},
                                     {"role": "user", "content": msg}]}
                async with client.stream("POST", url, json=body) as resp:
                    if resp.status_code in (402, 403, 409, 429):
                        await resp.aread()
                        stopped = {402: "budget", 403: "tool", 409: "quality",
                                   429: "loop"}[resp.status_code]
                        await publish("loop_stopped", stopped_by=stopped,
                                      **budget.as_dict(), **_quality_stats(),
                                      **loop_detector.stats())
                        return
                    body_txt = ""
                    async for line in resp.aiter_lines():
                        body_txt += line
                    if '"upstream_error"' in body_txt:
                        await publish("loop_stopped", stopped_by="error",
                                      **budget.as_dict(), **_quality_stats(),
                                      **loop_detector.stats())
                        return
                await asyncio.sleep(0.15)
        await publish("loop_stopped", stopped_by="max_iterations",
                      **budget.as_dict(), **_quality_stats(), **loop_detector.stats())
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        await publish("agent_error", detail=str(exc)[:300])


async def _stop_demo() -> None:
    global _demo_task
    if _demo_task is not None and not _demo_task.done():
        _demo_task.cancel()
        try:
            await _demo_task
        except BaseException:
            pass
    _demo_task = None


def _busy() -> bool:
    return _demo_task is not None and not _demo_task.done()


_mock_thread = None


def _ensure_mock() -> str:
    """Avvia il finto provider (solo per le demo simulate) se non gia' attivo.
    Se qualcuno lo sta gia' servendo su quella porta (es. run_demo.py), lo riusa
    invece di tentare un secondo avvio che fallirebbe con 'address in use'."""
    global _mock_thread
    url = "http://127.0.0.1:8931/v1"
    if _mock_thread is not None and _mock_thread.is_alive():
        return _mock_thread.base_url
    try:
        r = httpx.get("http://127.0.0.1:8931/health", timeout=1.5)
        if r.status_code == 200:
            return url          # gia' attivo: lo riusiamo
    except Exception:
        pass
    from mock_llm_server import MockServerThread
    _mock_thread = MockServerThread(port=8931)
    _mock_thread.start()
    _mock_thread.wait_until_ready()
    return _mock_thread.base_url


@app.post("/run/real")
async def run_real():
    """Fa CHIAMATE VERE al provider configurato, finche' il budget non finisce
    (o la qualita' non scatta). Nessun terminale: parte dal browser."""
    global _demo_task
    if _busy():
        return {"status": "already_running"}
    if not config.api_key:
        return JSONResponse(status_code=400,
                            content={"error": "Set your API key in the dashboard first."})
    _demo_task = asyncio.create_task(
        _loop_agent("Write one short sentence about the sea.",
                    "You are a helpful assistant.", config.model, max_iter=200))
    return {"status": "running"}


@app.post("/run/real_json")
async def run_real_json():
    """Chiamate VERE con una regola di qualita' impossibile da soddisfare per il
    modello (deve restituire JSON con campi specifici, ma gli chiediamo prosa):
    mostra il breaker di qualita' su traffico reale."""
    global _demo_task
    if _busy():
        return {"status": "already_running"}
    if not config.api_key:
        return JSONResponse(status_code=400,
                            content={"error": "Set your API key in the dashboard first."})
    config.check_policy = {"type": "json_schema",
                           "schema": {"type": "object", "required": ["name", "value"],
                                      "properties": {"name": {"type": "string"},
                                                     "value": {"type": "number"}}}}
    _reset_quality_counters()
    await publish("reset", **budget.as_dict(), **_quality_stats())
    _demo_task = asyncio.create_task(
        _loop_agent("Tell me a short story about the sea. Plain prose, no JSON.",
                    "You are a storyteller.", config.model, max_iter=200))
    return {"status": "running"}


@app.post("/demo/unleash")
async def unleash():
    """Demo SIMULATA (nessuna chiave necessaria): provider finto, budget piccolo."""
    global _demo_task
    if _busy():
        return {"status": "already_running"}
    config.check_policy = None
    config.upstream = _ensure_mock()
    _reset_quality_counters()
    async with _budget_lock:
        budget.limit = 0.05
        budget.spent = 0.0
        budget.calls = budget.refused = budget.halted = 0
    await publish("reset", **budget.as_dict(), **_quality_stats())
    _demo_task = asyncio.create_task(
        _loop_agent("Keep reasoning step by step. Never stop.",
                    "You are an autonomous agent.", "runaway-reasoner"))
    return {"status": "running"}


@app.post("/demo/unleash_quality")
async def unleash_quality():
    """Demo SIMULATA della qualita': budget generoso, output sempre invalido."""
    global _demo_task
    if _busy():
        return {"status": "already_running"}
    config.check_policy = {"type": "json_schema",
                           "schema": {"type": "object", "required": ["name", "value"],
                                      "properties": {"name": {"type": "string"},
                                                     "value": {"type": "number"}}}}
    config.trip_after = 2
    config.upstream = _ensure_mock()
    _reset_quality_counters()
    async with _budget_lock:
        budget.limit = 5.00
        budget.spent = 0.0
        budget.calls = budget.refused = budget.halted = 0
    await publish("reset", **budget.as_dict(), **_quality_stats())
    _demo_task = asyncio.create_task(
        _loop_agent("Return ONLY JSON {name, value}.",
                    "You extract structured data as strict JSON.", "runaway-reasoner"))
    return {"status": "running"}


@app.post("/demo/unleash_fallback")
async def unleash_fallback():
    """Demo SIMULATA del Quality-Triggered Fallback: il primario sbaglia sempre,
    il fallback risponde correttamente -> l'agente viene SALVATO, non fermato."""
    global _demo_task
    if _busy():
        return {"status": "already_running"}
    config.check_policy = {"type": "json_schema",
                           "schema": {"type": "object", "required": ["name", "value"],
                                      "properties": {"name": {"type": "string"},
                                                     "value": {"type": "number"}}}}
    config.trip_after = 2
    config.fallback_model = "good-model"      # modello "capace" del finto provider
    config.upstream = _ensure_mock()
    _reset_quality_counters()
    async with _budget_lock:
        budget.limit = 0.20
        budget.spent = 0.0
        budget.calls = budget.refused = budget.halted = 0
    await publish("reset", **budget.as_dict(), **_quality_stats())
    _demo_task = asyncio.create_task(
        _loop_agent("Return ONLY JSON {name, value}.",
                    "You extract structured data as strict JSON.",
                    "runaway-reasoner", max_iter=6))
    return {"status": "running"}


@app.post("/demo/unleash_loop")
async def unleash_loop():
    """Demo SIMULATA dello Stateful Loop Detection: un agente incastrato in un
    `while True` manda lo STESSO identico prompt. Budget generoso: un cap non lo
    fermerebbe. Il loop viene spezzato alla terza ripetizione, a spesa quasi nulla."""
    global _demo_task
    if _busy():
        return {"status": "already_running"}
    config.check_policy = None
    config.fallback_model = ""
    config.upstream = _ensure_mock()
    _reset_quality_counters()
    loop_detector.enabled = True
    loop_detector.threshold = 3
    async with _budget_lock:
        budget.limit = 5.00
        budget.spent = 0.0
        budget.calls = budget.refused = budget.halted = 0
    await publish("reset", **budget.as_dict(), **_quality_stats(), **loop_detector.stats())
    _demo_task = asyncio.create_task(
        _loop_agent("Check the order status and retry until it succeeds.",
                    "You are an autonomous agent.", "runaway-reasoner",
                    max_iter=12, identical=True))
    return {"status": "running"}


@app.post("/demo/unleash_tools")
async def unleash_tools():
    """Demo SIMULATA dell'Action Firewall: l'agente allucina e chiede
    `delete_database`. Il proxy scarta la chiamata: non arriva mai all'esecutore."""
    global _demo_task
    if _busy():
        return {"status": "already_running"}
    config.check_policy = None
    config.fallback_model = ""
    config.upstream = _ensure_mock()
    _reset_quality_counters()
    loop_detector.enabled = False        # qui vogliamo mostrare il firewall
    tool_firewall.configure(blocked=["delete_*", "drop_*", "refund", "transfer_funds"],
                            allowed=[], mode="block")
    async with _budget_lock:
        budget.limit = 5.00
        budget.spent = 0.0
        budget.calls = budget.refused = budget.halted = 0
    await publish("reset", **budget.as_dict(), **_quality_stats(),
                  **loop_detector.stats(), **tool_firewall.stats())

    async def run():
        url = "http://127.0.0.1:" + str(PROXY_PORT) + "/v1/chat/completions"
        tools = [{"type": "function",
                  "function": {"name": "delete_database",
                               "description": "Delete a database",
                               "parameters": {"type": "object"}}}]
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                for i in range(3):
                    await client.post(url, json={
                        "model": "tool-caller-danger", "tools": tools, "stream": False,
                        "messages": [{"role": "user",
                                      "content": "Clean up the stale records (attempt "
                                                 + str(i + 1) + ")"}]})
                    await asyncio.sleep(0.4)
                # poi un tool innocuo: deve passare
                await client.post(url, json={
                    "model": "tool-caller-safe", "tools": tools, "stream": False,
                    "messages": [{"role": "user", "content": "What's the weather?"}]})
            await publish("loop_stopped", stopped_by="tool", **budget.as_dict(),
                          **_quality_stats(), **tool_firewall.stats())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await publish("agent_error", detail=str(exc)[:300])

    _demo_task = asyncio.create_task(run())
    return {"status": "running"}


@app.post("/demo/reset")
async def reset():
    await _stop_demo()
    _reset_quality_counters()
    async with _budget_lock:
        budget.spent = 0.0
        budget.calls = budget.refused = budget.halted = 0
    await publish("reset", **budget.as_dict(), **_quality_stats())
    return budget.as_dict()


@app.post("/stop")
async def stop():
    await _stop_demo()
    await publish("loop_stopped", stopped_by="user", **budget.as_dict(), **_quality_stats())
    return {"status": "stopped"}


@app.get("/", response_class=HTMLResponse)
async def index():
    p = _HERE / "dashboard.html"
    if p.exists():
        return HTMLResponse(p.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>SpendGuard running.</h1><p>dashboard.html not found.</p>")


def _open_browser(url: str) -> None:
    def go():
        time.sleep(1.2)
        try:
            webbrowser.open(url)
        except Exception:
            pass
    threading.Thread(target=go, daemon=True).start()


if __name__ == "__main__":
    url = "http://127.0.0.1:" + str(PROXY_PORT)
    print("\n" + "=" * 62)
    print("  SpendGuard is running")
    print("=" * 62)
    print("  Dashboard:  " + url)
    print("  Configure everything there: provider, API key, budget, quality.")
    print("  Point your agent's base_url at:  " + url + "/v1")
    print("  (Ctrl+C to quit)")
    print("=" * 62 + "\n")
    _open_browser(url)
    uvicorn.run(app, host="127.0.0.1", port=PROXY_PORT, log_level="warning")