"""
================================================================================
 scan_secrets.py - run this BEFORE you publish anything
================================================================================

    python scan_secrets.py

Scans this folder (and, if present, the git history) for API keys, personal
paths, emails and other things you don't want on GitHub. Exits with code 1 if
anything suspicious is found, so you can also use it in CI.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Cartelle e file da non ispezionare (rumore, non codice sorgente)
SKIP_DIRS = {".git", "__pycache__", ".venv", "venv", "node_modules", ".idea", ".vscode"}
SKIP_SUFFIX = {".pyc", ".png", ".jpg", ".jpeg", ".gif", ".pdf", ".zip", ".exe", ".ico"}

# (nome, regex, gravita')
PATTERNS = [
    ("OpenAI key",        r"sk-(?!test|fake|dummy|set-your|proj-example)[A-Za-z0-9_-]{20,}", "CRITICAL"),
    ("Groq key",          r"gsk_[A-Za-z0-9]{20,}", "CRITICAL"),
    ("Anthropic key",     r"sk-ant-[A-Za-z0-9_-]{20,}", "CRITICAL"),
    ("Google API key",    r"AIza[0-9A-Za-z_-]{35}", "CRITICAL"),
    ("AWS access key",    r"AKIA[0-9A-Z]{16}", "CRITICAL"),
    ("GitHub token",      r"gh[pousr]_[A-Za-z0-9]{30,}", "CRITICAL"),
    ("Slack token",       r"xox[baprs]-[A-Za-z0-9-]{10,}", "CRITICAL"),
    ("Private key block", r"-----BEGIN (RSA |EC |OPENSSH |PGP )?PRIVATE KEY-----", "CRITICAL"),
    ("Bearer hardcoded",  r"[Bb]earer\s+[A-Za-z0-9._-]{20,}", "HIGH"),
    ("Assigned secret",   r"(?i)(api_key|apikey|secret|password|passwd|token)\s*[:=]\s*[\"'][^\"'\s]{12,}[\"']", "HIGH"),
    ("Windows user path", r"[Cc]:\\\\?Users\\\\?[A-Za-z0-9._-]+", "MEDIUM"),
    ("Unix home path",    r"/(home|Users)/[A-Za-z0-9._-]+/", "MEDIUM"),
    ("Email address",     r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "MEDIUM"),
]

# Valori palesemente finti: non devono generare falsi allarmi
SAFE_TOKENS = ("sk-test", "sk-fake", "sk-dummy", "sk-agent", "sk-set-your",
               "your-provider-key", "paste your", "example.com", "sk-la-tua")


def looks_safe(line: str) -> bool:
    low = line.lower()
    return any(tok in low for tok in SAFE_TOKENS)


def scan_file(path: Path) -> list[tuple[int, str, str, str]]:
    hits = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return hits
    for i, line in enumerate(text.splitlines(), 1):
        if looks_safe(line):
            continue
        for name, rx, sev in PATTERNS:
            if re.search(rx, line):
                shown = line.strip()
                if len(shown) > 110:
                    shown = shown[:110] + "..."
                hits.append((i, name, sev, shown))
                break
    return hits


def walk() -> list[Path]:
    out = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for f in filenames:
            p = Path(dirpath) / f
            if p.suffix.lower() in SKIP_SUFFIX or p.name == Path(__file__).name:
                continue
            if p.stat().st_size > 2_000_000:
                continue
            out.append(p)
    return out


def scan_git_history() -> list[str]:
    """Una chiave rimossa dai file resta nella cronologia: la cerchiamo li'."""
    if not (ROOT / ".git").exists():
        return []
    try:
        # encoding esplicito: su Windows la codifica di sistema (cp1252) va in
        # errore appena la cronologia contiene un byte che non sa decodificare.
        # errors="replace" sostituisce quei byte invece di far fallire tutto.
        proc = subprocess.run(["git", "log", "-p", "--all"], cwd=ROOT,
                              capture_output=True, timeout=180,
                              encoding="utf-8", errors="replace")
        blob = proc.stdout or ""
    except FileNotFoundError:
        print("  (git non trovato: cronologia non controllata)\n")
        return []
    except Exception as exc:
        print("  (impossibile leggere la cronologia git: " + str(exc)[:80] + ")\n")
        return []
    if not blob:
        return []
    found = []
    for name, rx, sev in PATTERNS:
        if sev != "CRITICAL":
            continue
        for m in re.finditer(rx, blob):
            frag = m.group(0)
            if not any(t in frag.lower() for t in SAFE_TOKENS):
                found.append(name + ": " + frag[:12] + "...")
    return sorted(set(found))


def main() -> int:
    files = walk()
    print("=" * 68)
    print("  SpendGuard - pre-publish secret scan")
    print("=" * 68)
    print("  scanning " + str(len(files)) + " files in " + str(ROOT))
    print()

    critical = high = medium = 0
    for p in sorted(files):
        hits = scan_file(p)
        if not hits:
            continue
        print("  " + str(p.relative_to(ROOT)))
        for line_no, name, sev, shown in hits:
            mark = {"CRITICAL": "!!", "HIGH": " !", "MEDIUM": " ."}[sev]
            print("   " + mark + " line " + str(line_no) + "  [" + sev + "] " + name)
            print("        " + shown)
            if sev == "CRITICAL":
                critical += 1
            elif sev == "HIGH":
                high += 1
            else:
                medium += 1
        print()

    hist = scan_git_history()
    if hist:
        print("  GIT HISTORY - secrets found in past commits:")
        for h in hist:
            print("   !! " + h)
        print("      Removing them from the files is NOT enough: rotate the key,")
        print("      and consider rewriting history (git filter-repo) or starting")
        print("      a fresh repository.")
        print()
        critical += len(hist)

    print("-" * 68)
    if critical:
        print("  RESULT: " + str(critical) + " CRITICAL finding(s). DO NOT PUBLISH.")
        print("          Rotate any exposed key immediately, then re-run this scan.")
    elif high:
        print("  RESULT: " + str(high) + " high-severity finding(s). Review before publishing.")
    elif medium:
        print("  RESULT: clean of keys; " + str(medium) + " minor finding(s) "
              "(paths/emails) worth a look.")
    else:
        print("  RESULT: clean. Nothing that looks like a secret.")
    print("-" * 68)
    return 1 if (critical or high) else 0


if __name__ == "__main__":
    raise SystemExit(main())