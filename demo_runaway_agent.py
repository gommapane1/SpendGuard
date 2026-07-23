"""
================================================================================
 demo_runaway_agent.py — watch SpendGuard stop a runaway agent, live
================================================================================

Lo scenario, che ogni sviluppatore di agenti riconosce: un agente entra in un
loop di "ragionamento" e non si ferma piu'. Ad ogni giro chiama l'LLM. Senza
guardrail, e' la bolletta a sorpresa da migliaia di dollari arrivata di notte.

Qui il provider e' un finto endpoint OpenAI-compatible che gira in locale (vedi
mock_llm_server.py) e simula un modello prolisso che IGNORA max_tokens. Il
client `openai` e' quello VERO: apre una connessione HTTP reale. In produzione
si cambia una riga (base_url) e si punta a OpenAI/Groq/Anthropic.

    python3 demo_runaway_agent.py
"""

from __future__ import annotations

import sys
import time

from openai import OpenAI

from mock_llm_server import MockServerThread
from spend_guard import SpendGuard, GuardEvent, BudgetExceededError

# ---- estetica terminale (colore solo se siamo su un vero terminale) ---------
_TTY = sys.stdout.isatty()


def _c(code: str, s: str) -> str:
    return f"\033[{code}m{s}\033[0m" if _TTY else s


def red(s): return _c("91;1", s)
def green(s): return _c("92;1", s)
def yellow(s): return _c("93;1", s)
def cyan(s): return _c("96", s)
def dim(s): return _c("90", s)
def bold(s): return _c("1", s)


# =============================================================================
#  Printer: trasforma gli eventi del firewall in un log visivo
# =============================================================================

class DemoPrinter:
    def __init__(self) -> None:
        self.call_no = 0
        self._mid_line = False

    def _newline_if_needed(self) -> None:
        if self._mid_line:
            sys.stdout.write("\n")
            self._mid_line = False

    def __call__(self, ev: GuardEvent) -> None:
        d = ev.detail
        if ev.kind == "call_start":
            self.call_no += 1
            self._newline_if_needed()
            remaining = dim(f"(remaining ${d['remaining_usd']:.4f})")
            print(
                f"{cyan('→ call #' + str(self.call_no))}  "
                f"model={d['model']}  "
                f"input={d['input_tokens']} tok  "
                f"{remaining}"
            )

        elif ev.kind == "progress":
            line = (
                f"   {dim('streaming…')} {d['output_tokens']:>4} tok  "
                f"live ${d['live_cost_usd']:.4f}  "
                f"session ${d['session_spent_usd']:.4f} / budget"
            )
            if _TTY:
                sys.stdout.write("\r" + line)
                sys.stdout.flush()
                self._mid_line = True
            else:
                sys.stdout.write(".")
                sys.stdout.flush()
                self._mid_line = True

        elif ev.kind == "halt":
            self._newline_if_needed()
            print(
                f"   {red('■ KILL-SWITCH')} — stream cut mid-generation at "
                f"{d['output_tokens']} tok  (would have kept going)  "
                f"spent capped at ${d['budget_usd']:.4f}"
            )

        elif ev.kind == "call_done":
            self._newline_if_needed()
            tag = red("halted") if d["halted"] else green("ok")
            remaining = dim(f"(remaining ${d['remaining_usd']:.4f})")
            print(
                f"   {tag}  cost ${d['cost_usd']:.4f}  "
                f"→ session ${d['spent_usd']:.4f}  "
                f"{remaining}"
            )

        elif ev.kind == "refused":
            self._newline_if_needed()
            print(
                f"{red('⛔ REFUSED before sending')}  "
                f"reason={d['reason']}  "
                f"need ${d['needed_usd']:.4f} but only ${d['remaining_usd']:.4f} left"
            )


# =============================================================================
#  La demo
# =============================================================================

def main() -> int:
    print(bold("\n" + "=" * 70))
    print(bold("  SpendGuard — runaway agent, real-time budget firewall"))
    print(bold("=" * 70))

    BUDGET = 0.02
    MODEL = "runaway-reasoner"
    SAFETY_MAX_LOOPS = 40  # rete di sicurezza: se la logica fallisse, non giriamo davvero all'infinito

    # 1) Avvia il finto provider in un thread.
    server = MockServerThread(port=8931)
    server.start()
    server.wait_until_ready()
    print(dim(f"  mock provider up at {server.base_url}  (simulates a model that ignores max_tokens)"))

    # 2) Client OpenAI VERO puntato al mock. La api_key e' finta: il mock la
    #    ignora, non e' un segreto. In produzione: cambia solo base_url e key.
    client = OpenAI(base_url=server.base_url, api_key="sk-fake-not-a-secret")

    # 3) Il firewall. Budget di sessione + prezzo del modello (indicativo).
    printer = DemoPrinter()
    guard = SpendGuard(client, budget_usd=BUDGET, on_event=printer, progress_every=45)
    guard.register_price(MODEL, input_per_1m=2.00, output_per_1m=6.00)

    print(f"\n  budget for this run: {bold(green(f'${BUDGET:.2f}'))}")
    print(dim("  an agent is now stuck in a reasoning loop and will call the LLM forever…\n"))
    time.sleep(0.4)

    # 4) L'agente impazzito: loop che senza guardrail non finirebbe mai.
    messages = [
        {"role": "system", "content": "You are an autonomous agent."},
        {"role": "user", "content": "Keep reasoning step by step. Do not stop until the task is fully solved."},
    ]

    stopped_by_guard = False
    for _ in range(SAFETY_MAX_LOOPS):
        try:
            guard.create(model=MODEL, messages=messages)
        except BudgetExceededError:
            # Il firewall ha rifiutato PRIMA di spendere: il loop e' spezzato qui.
            stopped_by_guard = True
            break
    else:
        print(yellow("\n  (safety cap hit — in a real runaway this loop never returns)"))

    # 5) Il verdetto.
    s = guard.summary()
    print(bold("\n" + "-" * 70))
    print(bold("  DAMAGE PREVENTED"))
    print(bold("-" * 70))
    print(f"  budget:            ${s['budget_usd']:.4f}")
    print(f"  actually spent:    ${s['spent_usd']:.4f}   {green('(never exceeded)')}")
    print(f"  calls completed:   {s['calls_completed']}")
    print(f"  streams halted:    {s['streams_halted']}   {dim('(cut mid-generation by the kill-switch)')}")
    print(f"  calls refused:     {s['calls_refused']}   {dim('(blocked before a single token was sent)')}")
    if stopped_by_guard:
        print(f"\n  {green('✔')} the loop was stopped by SpendGuard, not by an empty bank account.")
    print(bold("=" * 70) + "\n")

    server.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())