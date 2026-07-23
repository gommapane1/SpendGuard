"""
================================================================================
 spend_guard.py — a real-time spend firewall for autonomous LLM agents
================================================================================

The one thing this does, ruthlessly: it stops an agent BEFORE it bankrupts you.

Observability tools tell you *after* the money is gone. SpendGuard sits on the
wire between your agent and any OpenAI-compatible provider and enforces a hard
dollar budget in two places where money actually leaves your pocket:

  1. PRE-FLIGHT  — before a single token is sent, it prices the call. If even
                   the guaranteed input cost would blow the remaining budget,
                   the call is REFUSED. Nothing is spent. This is what stops a
                   runaway loop: the (N+1)-th call simply never fires.

  2. MID-STREAM  — it forces streaming and meters the cost token-by-token as
                   the response arrives. The instant cumulative spend hits the
                   cap, it CLOSES the stream mid-generation. The kill-switch.

Everything is provider-agnostic: point it at OpenAI, Groq, or Anthropic's
OpenAI-compat endpoint by changing one base_url. No blockchain, no dashboard,
no ecosystem to wait for. `pip install`, wrap your client, done.

--------------------------------------------------------------------------------
 NOTE ON PRICES: the built-in price book below is ILLUSTRATIVE and changes over
 time. In production you override it (register_price / pass `prices=`) and, ideally,
 sync it from the provider. The *mechanism* is what matters; the numbers are yours.
--------------------------------------------------------------------------------
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

# tiktoken ci serve per contare i token PRIMA di chiamare (input) e, soprattutto,
# DURANTE lo stream (output) -- cosi' il kill-switch funziona anche con provider
# che non restituiscono `usage` nello streaming. La stima e' esatta per i modelli
# OpenAI e una buona approssimazione per gli altri (llama, ecc.).
try:
    import tiktoken
    _HAS_TIKTOKEN = True
except Exception:
    _HAS_TIKTOKEN = False


def _approx_tokens(text: str) -> int:
    # Euristica di ripiego: ~4 caratteri per token (regola pratica di OpenAI).
    # Non e' precisa, ma un firewall di budget deve SEMPRE poter contare
    # qualcosa: meglio una stima che un crash che ti lascia senza protezione.
    if not text:
        return 0
    return max(1, math.ceil(len(text) / 4))


# Cache degli encoder per modello (crearli ad ogni chiamata sarebbe uno spreco).
_ENCODER_CACHE: dict[str, Any] = {}
# Se il vocabolario tiktoken non e' caricabile (es. rete assente, ambiente
# air-gapped o ristretto) alziamo questo flag e passiamo all'euristica per
# sempre, senza riprovare download che falliranno.
_TIKTOKEN_BROKEN = False


def _count_tokens(text: str, model: str) -> int:
    """Conta i token in modo preciso con tiktoken; se non e' disponibile o non
    riesce a caricare il vocabolario, ripiega sull'euristica senza mai fallire."""
    global _TIKTOKEN_BROKEN
    if not text:
        return 0
    if not _HAS_TIKTOKEN or _TIKTOKEN_BROKEN:
        return _approx_tokens(text)

    enc = _ENCODER_CACHE.get(model)
    if enc is None:
        try:
            try:
                enc = tiktoken.encoding_for_model(model)
            except KeyError:
                # Modello non noto a tiktoken (es. llama/Groq): usiamo o200k_base,
                # l'encoding dei modelli OpenAI recenti -- abbastanza vicino per
                # il budget.
                enc = tiktoken.get_encoding("o200k_base")
        except Exception:
            _TIKTOKEN_BROKEN = True
            return _approx_tokens(text)
        _ENCODER_CACHE[model] = enc

    try:
        return len(enc.encode(text))
    except Exception:
        _TIKTOKEN_BROKEN = True
        return _approx_tokens(text)


# =============================================================================
#  Price book — USD per 1M token, come (input, output). Override a piacere.
# =============================================================================

# I prezzi sono in dollari per 1.000.000 di token. Valori indicativi: verifica
# sempre i prezzi correnti del tuo provider e sovrascrivili.
DEFAULT_PRICES_PER_1M: dict[str, tuple[float, float]] = {
    # OpenAI (indicativi)
    "gpt-4o": (2.50, 10.00),
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4.1": (2.00, 8.00),
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-3.5-turbo": (0.50, 1.50),
    # Groq / llama (indicativi)
    "llama-3.1-8b-instant": (0.05, 0.08),
    "llama-3.3-70b-versatile": (0.59, 0.79),
    # Anthropic via OpenAI-compat (indicativi)
    "claude-sonnet-4-6": (3.00, 15.00),
}

# Prezzo di ripiego per modelli sconosciuti: volutamente NON gratis, cosi' il
# budget protegge anche i modelli che non abbiamo mappato.
FALLBACK_PRICE_PER_1M: tuple[float, float] = (1.00, 3.00)


# =============================================================================
#  Errori ed esiti
# =============================================================================

class BudgetExceededError(Exception):
    """Sollevata PRIMA di spendere quando la chiamata non entra nel budget."""

    def __init__(self, message: str, *, remaining_usd: float, needed_usd: float):
        super().__init__(message)
        self.remaining_usd = remaining_usd
        self.needed_usd = needed_usd


@dataclass
class CallResult:
    """Esito di una singola chiamata passata dal firewall."""
    text: str
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    halted: bool                 # True se il kill-switch ha tagliato lo stream
    finish_reason: Optional[str] # 'stop', 'length', o None se interrotto da noi
    latency_s: float


@dataclass
class GuardEvent:
    """Evento osservabile emesso dal firewall (per log, UI, video demo...)."""
    kind: str          # 'call_start' | 'refused' | 'halt' | 'call_done'
    detail: dict[str, Any] = field(default_factory=dict)


# =============================================================================
#  SpendGuard — il firewall
# =============================================================================

class SpendGuard:
    """
    Avvolge un client OpenAI-compatible e impone un budget massimo in dollari
    sull'INTERA sessione (somma di tutte le chiamate).

    Uso minimo:
        from openai import OpenAI
        client = OpenAI(base_url=..., api_key=...)
        guard = SpendGuard(client, budget_usd=5.00)
        result = guard.create(model="gpt-4o-mini", messages=[...])
        print(result.text, result.cost_usd)
    """

    def __init__(
        self,
        client: Any,
        *,
        budget_usd: float,
        prices_per_1m: Optional[dict[str, tuple[float, float]]] = None,
        on_event: Optional[Callable[[GuardEvent], None]] = None,
        progress_every: int = 0,
    ) -> None:
        if budget_usd <= 0:
            raise ValueError("budget_usd deve essere > 0")
        # Emette un evento 'progress' ogni ~N token di output (0 = disattivato).
        # Utile per UI e per la demo: mostra il costo che sale in diretta.
        self._progress_every = int(progress_every)
        self._client = client
        self.budget_usd = float(budget_usd)
        self.spent_usd = 0.0
        # Copia il price book di default e applica gli override dell'utente.
        self._prices = dict(DEFAULT_PRICES_PER_1M)
        if prices_per_1m:
            self._prices.update(prices_per_1m)
        self._on_event = on_event
        self.ledger: list[CallResult] = []
        self.refused_calls = 0

    # ---- prezzi -------------------------------------------------------------

    def register_price(self, model: str, input_per_1m: float, output_per_1m: float) -> None:
        """Registra/sovrascrive il prezzo di un modello (USD per 1M token)."""
        self._prices[model] = (float(input_per_1m), float(output_per_1m))

    def _price(self, model: str) -> tuple[float, float]:
        # Prezzo per singolo token (non per milione), pronto da moltiplicare.
        in_1m, out_1m = self._prices.get(model, FALLBACK_PRICE_PER_1M)
        return in_1m / 1_000_000.0, out_1m / 1_000_000.0

    # ---- utilita' budget ----------------------------------------------------

    @property
    def remaining_usd(self) -> float:
        return max(0.0, self.budget_usd - self.spent_usd)

    def _emit(self, kind: str, **detail: Any) -> None:
        if self._on_event is not None:
            self._on_event(GuardEvent(kind=kind, detail=detail))

    @staticmethod
    def _messages_to_text(messages: list[dict[str, Any]]) -> str:
        # Concatenazione grossolana del contenuto dei messaggi per contare i
        # token di input. Sufficiente per il gating di budget.
        parts: list[str] = []
        for m in messages:
            content = m.get("content", "")
            if isinstance(content, str):
                parts.append(content)
            elif isinstance(content, list):
                # formato multimodale: prendiamo solo i pezzi testuali
                for chunk in content:
                    if isinstance(chunk, dict) and chunk.get("type") == "text":
                        parts.append(chunk.get("text", ""))
        return "\n".join(parts)

    # ---- il metodo che conta: create() --------------------------------------

    def create(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> CallResult:
        """
        Esegue una chat completion sotto controllo di budget.

        Passi:
          1. PRE-FLIGHT: prezza l'input. Se il solo input sfonda il budget
             residuo -> BudgetExceededError, zero speso.
          2. Calcola quanti token di output il budget residuo puo' permettersi
             e usa quel valore come tetto reale a max_tokens.
          3. Forza lo streaming e conta i token in arrivo. Appena la spesa
             cumulata raggiunge il cap -> chiude lo stream (kill-switch).
        """
        price_in, price_out = self._price(model)
        text = self._messages_to_text(messages)
        input_tokens = _count_tokens(text, model)
        input_cost = input_tokens * price_in
        remaining = self.remaining_usd

        # -- 1. PRE-FLIGHT: rifiuto PRIMA di spendere un solo token ------------
        if input_cost > remaining:
            self.refused_calls += 1
            self._emit(
                "refused",
                model=model,
                reason="input_exceeds_budget",
                remaining_usd=remaining,
                needed_usd=input_cost,
            )
            raise BudgetExceededError(
                f"Refused before sending: input alone costs ${input_cost:.6f}, "
                f"only ${remaining:.6f} left in budget.",
                remaining_usd=remaining,
                needed_usd=input_cost,
            )

        # -- 2. quanti token di output possiamo permetterci? ------------------
        budget_for_output = remaining - input_cost
        if price_out > 0:
            max_affordable_out = int(budget_for_output // price_out)
        else:
            max_affordable_out = 10_000_000  # output gratis: nessun limite di costo
        if max_affordable_out <= 0:
            # L'input entra ma non resta nulla nemmeno per un token di risposta.
            self.refused_calls += 1
            self._emit(
                "refused",
                model=model,
                reason="no_budget_for_output",
                remaining_usd=remaining,
                needed_usd=input_cost + price_out,
            )
            raise BudgetExceededError(
                f"Refused before sending: input fits (${input_cost:.6f}) but no "
                f"budget left to generate a reply.",
                remaining_usd=remaining,
                needed_usd=input_cost + price_out,
            )

        hard_cap_out = min(max_tokens, max_affordable_out) if max_tokens else max_affordable_out

        self._emit(
            "call_start",
            model=model,
            input_tokens=input_tokens,
            input_cost_usd=input_cost,
            output_token_cap=hard_cap_out,
            remaining_usd=remaining,
        )

        # -- 3. streaming forzato + metering + kill-switch --------------------
        started = time.monotonic()
        # stream_options include_usage: se il provider lo supporta ci da i token
        # esatti nel chunk finale (per riconciliare il ledger). Il kill-switch
        # NON dipende da questo: conta i token localmente con tiktoken.
        stream = self._client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True,
            max_tokens=hard_cap_out,
            stream_options={"include_usage": True},
            **kwargs,
        )

        output_tokens = 0
        since_last_progress = 0
        collected: list[str] = []
        halted = False
        finish_reason: Optional[str] = None
        provider_output_tokens: Optional[int] = None

        try:
            for chunk in stream:
                choices = getattr(chunk, "choices", None) or []
                if choices:
                    delta = choices[0].delta
                    piece = getattr(delta, "content", None)
                    if piece:
                        delta_tokens = _count_tokens(piece, model)
                        prospective = input_cost + (output_tokens + delta_tokens) * price_out
                        # KILL-SWITCH: se questo token sfonderebbe, tagliamo PRIMA di
                        # contarlo o aggiungerlo -> la spesa non supera mai il budget.
                        if self.spent_usd + prospective >= self.budget_usd:
                            halted = True
                            self._emit(
                                "halt",
                                model=model,
                                output_tokens=output_tokens,
                                cost_usd=input_cost + output_tokens * price_out,
                                budget_usd=self.budget_usd,
                            )
                            break
                        collected.append(piece)
                        output_tokens += delta_tokens
                        since_last_progress += delta_tokens
                        live_cost = input_cost + output_tokens * price_out
                        # Avanzamento in diretta (per UI/demo): costo che sale.
                        if self._progress_every and since_last_progress >= self._progress_every:
                            since_last_progress = 0
                            self._emit(
                                "progress",
                                output_tokens=output_tokens,
                                live_cost_usd=live_cost,
                                session_spent_usd=self.spent_usd + live_cost,
                                remaining_usd=max(0.0, self.budget_usd - self.spent_usd - live_cost),
                            )
                    fr = getattr(choices[0], "finish_reason", None)
                    if fr:
                        finish_reason = fr
                # Chunk finale con usage (se il provider lo manda).
                usage = getattr(chunk, "usage", None)
                if usage is not None:
                    provider_output_tokens = getattr(usage, "completion_tokens", None)
        finally:
            # Chiude la connessione HTTP: se eravamo a meta' stream, il provider
            # smette davvero di generare (e di farci pagare).
            try:
                stream.close()
            except Exception:
                pass

        # Riconciliazione: se il provider ci ha dato i token esatti e NON abbiamo
        # tagliato a meta', preferiamo il suo conteggio per il ledger.
        if provider_output_tokens is not None and not halted:
            output_tokens = provider_output_tokens

        cost = input_cost + output_tokens * price_out
        self.spent_usd += cost
        latency = time.monotonic() - started

        result = CallResult(
            text="".join(collected),
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
            halted=halted,
            finish_reason=None if halted else finish_reason,
            latency_s=latency,
        )
        self.ledger.append(result)
        self._emit(
            "call_done",
            model=model,
            cost_usd=cost,
            output_tokens=output_tokens,
            halted=halted,
            spent_usd=self.spent_usd,
            remaining_usd=self.remaining_usd,
        )
        return result

    # ---- riepilogo ----------------------------------------------------------

    def summary(self) -> dict[str, Any]:
        """Riepilogo di sessione, comodo per stampe e per la UI."""
        halted = sum(1 for c in self.ledger if c.halted)
        return {
            "budget_usd": self.budget_usd,
            "spent_usd": self.spent_usd,
            "remaining_usd": self.remaining_usd,
            "calls_completed": len(self.ledger),
            "calls_refused": self.refused_calls,
            "streams_halted": halted,
        }