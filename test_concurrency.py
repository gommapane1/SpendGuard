"""Verifica la correzione del bug di concorrenza + set-budget + singolo demo."""
import asyncio, threading, time, json
import httpx, uvicorn
from mock_llm_server import MockServerThread
import spend_proxy

mock = MockServerThread(port=8931); mock.start(); mock.wait_until_ready()
spend_proxy.UPSTREAM_BASE_URL = mock.base_url
cfg = uvicorn.Config(spend_proxy.app, host="127.0.0.1", port=8900, log_level="error")
server = uvicorn.Server(cfg); server.install_signal_handlers = lambda: None
threading.Thread(target=server.run, daemon=True).start()
while not getattr(server, "started", False): time.sleep(0.05)

BASE = "http://127.0.0.1:8900"
budget = spend_proxy.budget

async def one_call(client, i):
    body = {"model": "runaway-reasoner",
            "messages": [{"role": "user", "content": f"agent {i} reasoning forever"}],
            "stream": True}
    async with client.stream("POST", BASE + "/v1/chat/completions", json=body) as r:
        if r.status_code == 402:
            return "402"
        async for _ in r.aiter_lines():
            pass
        return "200"

async def main():
    # 1) 10 agenti IN PARALLELO sullo stesso budget (== premere unleash 10 volte)
    httpx.post(BASE + "/config/budget", json={"budget_usd": 0.05})
    print("budget set to $0.0500")
    async with httpx.AsyncClient(timeout=None) as client:
        results = await asyncio.gather(*[one_call(client, i) for i in range(10)])
    print("10 concurrent calls ->", {r: results.count(r) for r in set(results)})
    print(f"spent after concurrency: ${budget.spent:.6f} / ${budget.limit:.4f}")
    assert budget.spent <= budget.limit + 1e-9, "OVERSPEND under concurrency!"
    print("no overshoot under concurrency : PASS")

    # 2) set-budget azzera e riparte
    d = httpx.post(BASE + "/config/budget", json={"budget_usd": 0.02}).json()
    assert d["limit_usd"] == 0.02 and d["spent_usd"] == 0.0
    print("set-budget resets + applies     : PASS")

    # 3) input invalido -> 400
    code = httpx.post(BASE + "/config/budget", json={"budget_usd": -1}).status_code
    assert code == 400
    print("invalid budget rejected (400)   : PASS")

    # 4) doppio unleash -> il secondo e' already_running
    r1 = httpx.post(BASE + "/demo/unleash").json()["status"]
    r2 = httpx.post(BASE + "/demo/unleash").json()["status"]
    print("double unleash statuses:", r1, "/", r2)
    assert r2 == "already_running"
    print("single demo agent enforced      : PASS")
    httpx.post(BASE + "/demo/reset")

    print("\nALL CONCURRENCY CHECKS PASSED ✓")

asyncio.run(main())
server.should_exit = True