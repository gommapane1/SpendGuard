"""
================================================================================
 run_demo.py — one command to run the whole SpendGuard proxy demo
================================================================================

    python3 run_demo.py

Then open the URL it prints in your browser and click "Unleash runaway agent".

What it does:
  1. starts a FAKE OpenAI-compatible provider locally (no real API key, no real
     spend) that simulates a verbose model ignoring max_tokens;
  2. starts the SpendGuard proxy in front of it;
  3. serves a live dashboard where you watch the spend climb and the kill-switch
     fire, ending with an HTTP 402 that stops the runaway agent.

To point at a REAL provider instead of the fake one, set an env var, e.g.:
    SPENDGUARD_UPSTREAM=https://api.groq.com/openai/v1 python3 run_demo.py
(and have your agent send its real Authorization header through the proxy).
"""

from __future__ import annotations

import uvicorn

from mock_llm_server import MockServerThread
import spend_proxy


def main() -> int:
    # 1) finto upstream in un thread
    mock = MockServerThread(port=8931)
    mock.start()
    mock.wait_until_ready()

    # 2) il proxy punta al finto upstream (default gia' coerente)
    spend_proxy.UPSTREAM_BASE_URL = mock.base_url

    url = f"http://127.0.0.1:{spend_proxy.PROXY_PORT}"
    print("\n" + "=" * 66)
    print("  SpendGuard proxy demo is live")
    print("=" * 66)
    print(f"  fake provider (upstream): {mock.base_url}")
    print(f"  budget for this run:      ${spend_proxy.BUDGET_USD:.2f}")
    print()
    print(f"  >>> OPEN THIS IN YOUR BROWSER:  {url}")
    print()
    print("  then click 'Unleash runaway agent' and watch it get stopped.")
    print("  (Ctrl+C here to quit.)")
    print("=" * 66 + "\n")

    # 3) il proxy sul main thread (serve dashboard + endpoint)
    uvicorn.run(spend_proxy.app, host="127.0.0.1", port=spend_proxy.PROXY_PORT,
                log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())