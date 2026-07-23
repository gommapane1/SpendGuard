"""
================================================================================
 mock_llm_server.py — a fake OpenAI-compatible endpoint (for the demo only)
================================================================================

Perche' esiste: per DIMOSTRARE il kill-switch senza usare credenziali vere ne'
bruciare soldi veri, ci serve un provider che parli il protocollo di OpenAI ma
sia sotto il nostro controllo. Il vero client `openai` non sa che e' finto: apre
una connessione HTTP reale e legge lo stream SSE riga per riga, esattamente come
farebbe con api.openai.com. Lo scambio di provider in produzione e' UNA riga:
basta cambiare `base_url`.

Comportamento scelto di proposito: questo modello simula un "ragionatore"
prolisso che IGNORA `max_tokens` e continua a sfornare token. E' un caso reale
(molti modelli di reasoning o endpoint OSS non rispettano i limiti): e' proprio
lo scenario in cui un cap lato-client non basta e serve un kill-switch che conti
la spesa in tempo reale e stacchi la connessione.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()

# Ultima richiesta ricevuta: serve ai test per verificare cosa invia il proxy
# (es. che max_tokens non venga gonfiato -> bug del 413 "Request too large").
LAST_REQUEST: dict = {}

# Quanti "token" (parole) sputa il modello a prescindere da max_tokens.
RUNAWAY_TOKEN_COUNT = 600
# Ritardo per token: tiene lo stream "vivo" abbastanza da vedere il costo salire.
PER_TOKEN_DELAY_S = 0.004

# Modelli che emettono TOOL CALLS in streaming (per l'Action Firewall).
# Il nome arriva nel primo chunk, gli argomenti a pezzi: e' esattamente il
# formato di OpenAI, cosi' il firewall si prova sul caso reale.
TOOL_MODELS = {
    "tool-caller-danger": "delete_database",
    "tool-caller-safe": "get_weather",
    # Coppia CONCORDE: stesso tool, stessi argomenti -> il consenso passa.
    "consensus-a": "refund_order",
    "consensus-b": "refund_order",
    # Modello che ALLUCINA un'azione diversa -> il consenso fallisce.
    "consensus-hallucinating": "close_account",
}

# Argomenti per modello: se due modelli producono argomenti diversi sullo stesso
# tool, il consenso deve accorgersene.
TOOL_ARGS = {
    "consensus-a": ('{"', 'order_id', '": "', 'A-1', '", "', 'amount', '": ', '50', '}'),
    "consensus-b": ('{"', 'order_id', '": "', 'A-1', '", "', 'amount', '": ', '50', '}'),
    "consensus-divergent": ('{"', 'order_id', '": "', 'A-1', '", "', 'amount',
                            '": ', '5000', '}'),
}
# Modello che chiama lo stesso tool ma con importo diverso -> disaccordo sui parametri.
TOOL_MODELS["consensus-divergent"] = "refund_order"

# Modelli che si comportano "bene": restituiscono JSON valido invece di prosa.
# Servono a dimostrare il Quality-Triggered Fallback (il modello primario sbaglia,
# il fallback risponde correttamente e salva la chiamata).
GOOD_MODELS = {"good-model", "smart-fallback"}
_GOOD_JSON_TOKENS = ['{"', 'name', '":', ' "', 'sea', '",', ' "', 'value',
                     '":', ' 42', '}']

# Vocabolario finto da "catena di pensiero", parole separate da spazio cosi'
# ognuna e' circa un token per il conteggio.
_LEXICON = (
    "let me think about this step by step therefore we consider the next "
    "possibility carefully and re-examine our earlier assumption which suggests "
    "that the optimal path requires further reflection so continuing the analysis "
    "we note that each branch must be evaluated again and again until convergence"
).split()


def _chunk(model: str, *, role: str | None = None, content: str | None = None,
           tool_calls: list | None = None,
           finish_reason: str | None = None, usage: dict | None = None) -> str:
    """Costruisce una riga SSE nel formato ChatCompletionChunk di OpenAI."""
    delta: dict = {}
    if role is not None:
        delta["role"] = role
    if content is not None:
        delta["content"] = content
    if tool_calls is not None:
        delta["tool_calls"] = tool_calls
    choices = [] if usage is not None else [{
        "index": 0,
        "delta": delta,
        "finish_reason": finish_reason,
    }]
    payload = {
        "id": "chatcmpl-mock-runaway",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": choices,
    }
    if usage is not None:
        payload["usage"] = usage
    return f"data: {json.dumps(payload)}\n\n"


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.json()
    LAST_REQUEST.clear()
    LAST_REQUEST.update(body)
    model = body.get("model", "runaway-reasoner")
    want_usage = bool((body.get("stream_options") or {}).get("include_usage"))

    # Stima grezza dei prompt token (solo per il campo usage; non e' il punto).
    prompt_text = " ".join(
        m.get("content", "") if isinstance(m.get("content"), str) else ""
        for m in body.get("messages", [])
    )
    prompt_tokens = max(1, len(prompt_text.split()))

    async def event_stream():
        # Primo chunk: ruolo assistant (come fa OpenAI).
        yield _chunk(model, role="assistant")
        emitted = 0

        # Modello che chiama un TOOL: nome nel primo chunk, argomenti a pezzi
        # (identico al comportamento reale di OpenAI).
        if model in TOOL_MODELS:
            tool_name = TOOL_MODELS[model]
            yield _chunk(model, tool_calls=[{
                "index": 0, "id": "call_mock_1", "type": "function",
                "function": {"name": tool_name, "arguments": ""}}])
            await asyncio.sleep(PER_TOKEN_DELAY_S)
            args = TOOL_ARGS.get(model, ('{"', 'target', '": "', 'production',
                                         '", "', 'confirm', '": ', 'true', '}'))
            for frag in args:
                emitted += 1
                yield _chunk(model, tool_calls=[{
                    "index": 0, "function": {"arguments": frag}}])
                await asyncio.sleep(PER_TOKEN_DELAY_S)
            yield _chunk(model, finish_reason="tool_calls")
            if want_usage:
                yield _chunk(model, usage={
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": emitted,
                    "total_tokens": prompt_tokens + emitted,
                })
            yield "data: [DONE]\n\n"
            return

        # Modello "buono": risponde con JSON valido e si ferma correttamente.
        if model in GOOD_MODELS:
            for tok in _GOOD_JSON_TOKENS:
                emitted += 1
                yield _chunk(model, content=tok)
                await asyncio.sleep(PER_TOKEN_DELAY_S)
            yield _chunk(model, finish_reason="stop")
            if want_usage:
                yield _chunk(model, usage={
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": emitted,
                    "total_tokens": prompt_tokens + emitted,
                })
            yield "data: [DONE]\n\n"
            return

        # NOTA: ignoriamo deliberatamente body["max_tokens"]. Simuliamo un
        # provider che sfora. E' qui che il kill-switch lato-client conta.
        for i in range(RUNAWAY_TOKEN_COUNT):
            word = _LEXICON[i % len(_LEXICON)] + " "
            emitted += 1
            yield _chunk(model, content=word)
            await asyncio.sleep(PER_TOKEN_DELAY_S)
        # Se arriviamo in fondo (nessun kill), chiudiamo con finish_reason.
        yield _chunk(model, finish_reason="length")
        if want_usage:
            yield _chunk(model, usage={
                "prompt_tokens": prompt_tokens,
                "completion_tokens": emitted,
                "total_tokens": prompt_tokens + emitted,
            })
        yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/health")
async def health():
    return {"status": "ok"}


# =============================================================================
#  Avvio del server in un thread, senza signal handler (fuori dal main thread)
# =============================================================================

class MockServerThread(threading.Thread):
    """Fa girare uvicorn in un thread demone cosi' la demo resta un solo file."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8931) -> None:
        self.host = host
        self.port = port
        config = uvicorn.Config(app, host=host, port=port, log_level="warning")
        self.server = uvicorn.Server(config)
        # I signal handler di uvicorn funzionano solo nel main thread: li
        # disattiviamo, il thread e' demone e muore con il processo.
        self.server.install_signal_handlers = lambda: None
        super().__init__(daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/v1"

    def run(self) -> None:
        self.server.run()

    def wait_until_ready(self, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if getattr(self.server, "started", False):
                return
            time.sleep(0.05)
        raise RuntimeError("mock server non partito in tempo")

    def stop(self) -> None:
        self.server.should_exit = True