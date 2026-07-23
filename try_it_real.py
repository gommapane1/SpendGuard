"""
================================================================================
 try_it_real.py — send REAL calls through SpendGuard (safe-by-default)
================================================================================

This file never contains a key. It reads one from the environment, or from a
local `.env` file that .gitignore keeps out of your repository.

HOW TO RUN
----------
1. Start SpendGuard:            python spend_proxy.py
2. Create a file named `.env`  next to this script:

       SPENDGUARD_KEY=your-real-provider-key
       SPENDGUARD_MODEL=llama-3.1-8b-instant
       SPENDGUARD_UPSTREAM=https://api.groq.com/openai/v1

3. Run:                         python try_it_real.py

`.env` is listed in .gitignore, so it is never published. If you would rather
not create a file at all, set the variables in your shell instead:

    Windows PowerShell:   $env:SPENDGUARD_KEY="..."
    macOS / Linux:        export SPENDGUARD_KEY=...

NOTE: `export` does NOT work in PowerShell — that is why the `.env` file exists.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx
from openai import OpenAI

PROXY = os.environ.get("SPENDGUARD_PROXY", "http://127.0.0.1:8900")
HERE = Path(__file__).resolve().parent


def load_env_file() -> None:
    """Legge un .env locale (KEY=value). Non usa librerie esterne."""
    path = HERE / ".env"
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip("'\""))


load_env_file()

KEY = os.environ.get("SPENDGUARD_KEY") or os.environ.get("OPENAI_API_KEY") or ""
MODEL = os.environ.get("SPENDGUARD_MODEL", "gpt-4o-mini")

if not KEY:
    print("No API key found.\n")
    print("Create a file named .env next to this script containing:")
    print("    SPENDGUARD_KEY=your-real-provider-key")
    print("    SPENDGUARD_MODEL=" + MODEL)
    print("\n(.env is git-ignored, so it will never be published.)")
    sys.exit(1)

# Guardia di sicurezza: se qualcuno incolla una chiave DENTRO questo file, ce ne
# accorgiamo e ci fermiamo, invece di lasciarla finire su GitHub.
_source = Path(__file__).read_text(encoding="utf-8")
if KEY and KEY in _source:
    print("SAFETY STOP: your API key appears to be written inside this file.")
    print("Remove it and put it in .env instead — otherwise it gets published.")
    sys.exit(1)

# L'unica differenza rispetto al codice normale: base_url punta a SpendGuard.
client = OpenAI(base_url=PROXY + "/v1", api_key=KEY)

print("Real calls to '" + MODEL + "' through SpendGuard at " + PROXY)
print("Open " + PROXY + " to watch them live.\n")

for i in range(1, 500):
    try:
        r = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": "Write one short sentence about the sea."}],
        )
        try:
            s = httpx.get(PROXY + "/stats", timeout=10).json()
            spent = "spent $" + format(s["spent_usd"], ".6f") + " / $" + format(s["limit_usd"], ".4f")
        except Exception:
            spent = ""
        answer = (r.choices[0].message.content or "").strip().replace("\n", " ")
        print("call " + str(i).rjust(3) + ": ok   " + spent + "   | " + answer[:50])
    except Exception as exc:
        print("\ncall " + str(i).rjust(3) + ": STOPPED by SpendGuard -> " + type(exc).__name__)
        print("  " + str(exc)[:200])
        break

print("\nThat was real traffic to a real provider, capped by SpendGuard.")