import asyncio
import json
import uuid
from unittest.mock import patch

import httpx
import pytest

import main
from resilience import CircuitBreaker, ServiceUnavailable


def test_breaker_skips_requests_and_allows_single_probe():
    async def scenario():
        breaker = CircuitBreaker("Flight Service", failure_threshold=2)
        clock = [0.0]
        calls = []
        probe_started = asyncio.Event()
        release_probe = asyncio.Event()

        async def answer(request):
            calls.append(request.url.path)
            if len(calls) <= 2:
                return httpx.Response(500)
            probe_started.set()
            await release_probe.wait()
            return httpx.Response(404)

        async with httpx.AsyncClient(transport=httpx.MockTransport(answer)) as client:
            with patch("resilience.time") as fake_time:
                fake_time.monotonic.side_effect = lambda: clock[0]
                for _ in range(3):
                    with pytest.raises(ServiceUnavailable):
                        await breaker.get(client, "http://flight/flight")
                assert len(calls) == 2

                clock[0] = 5.0
                probe = asyncio.create_task(breaker.get(client, "http://flight/flight"))
                await asyncio.wait_for(probe_started.wait(), timeout=1)
                with pytest.raises(ServiceUnavailable):
                    await breaker.get(client, "http://flight/flight")
                assert len(calls) == 3
                release_probe.set()
                assert (await probe).status_code == 404
                assert (await breaker.get(client, "http://flight/flight")).status_code == 404
                assert len(calls) == 4

    asyncio.run(scenario())


def test_failed_purchase_rolls_back_and_return_retries(monkeypatch):
    monkeypatch.setattr(main, "FLIGHT_URL", "http://flight:8060")
    monkeypatch.setattr(main, "TICKET_URL", "http://ticket:8070")
    monkeypatch.setattr(main, "BONUS_URL", "http://bonus:8050")
    monkeypatch.setattr(main, "bonus_breaker", CircuitBreaker(main.BONUS_SERVICE))

    async def scenario():
        headers = {"X-User-Name": "Test Max"}
        payload = {"flightNumber": "AFL031", "price": 1500, "paidFromBalance": False}
        tickets = {}
        bonus_online = False
        refund_succeeded = asyncio.Event()
        failed_refund = asyncio.Event()

        async def downstream(request):
            path = request.url.path
            method = request.method
            if request.url.port == 8060:
                return httpx.Response(200, json={"date": "2021-10-08 20:00"})
            if request.url.port == 8070:
                if path.startswith("/internal/"):
                    tickets.pop(path.rsplit("/", 1)[-1])
                    return httpx.Response(204)
                if method == "POST":
                    ticket = json.loads(request.content)
                    ticket["status"] = "PAID"
                    tickets[ticket["ticketUid"]] = ticket
                    return httpx.Response(200, json=ticket)
                if method == "DELETE":
                    ticket = tickets[path.rsplit("/", 1)[-1]]
                    ticket["status"] = "CANCELED"
                    return httpx.Response(200)
                return httpx.Response(200, json=list(tickets.values()))
            if not bonus_online:
                if method == "DELETE":
                    failed_refund.set()
                raise httpx.ConnectError("service stopped", request=request)
            if method == "DELETE":
                assert request.headers["X-User-Name"] == "Test Max"
                refund_succeeded.set()
                return httpx.Response(200)
            return httpx.Response(200, json={"balance": 1650, "status": "GOLD"})

        original_client = httpx.AsyncClient
        async with original_client(transport=httpx.ASGITransport(app=main.app), base_url="http://gateway") as client:
            transport = httpx.MockTransport(downstream)
            monkeypatch.setattr(main.httpx, "AsyncClient", lambda **kwargs: original_client(transport=transport, **kwargs))
            monkeypatch.setattr(main, "RETRY_INTERVAL", 0.01)
            async with main.app.router.lifespan_context(main.app):
                response = await client.post("/api/v1/tickets", json=payload, headers=headers)
                assert response.status_code == 503
                assert response.json() == {"message": "Bonus Service unavailable"}
                assert not tickets
                response = await client.get("/api/v1/me", headers=headers)
                assert response.json() == {"tickets": [], "privilege": {}}

                ticket_uid = str(uuid.uuid4())
                tickets[ticket_uid] = {"status": "PAID"}
                response = await client.delete(f"/api/v1/tickets/{ticket_uid}", headers=headers)
                assert response.status_code == 204
                assert tickets[ticket_uid]["status"] == "CANCELED"
                assert failed_refund.is_set()
                assert not refund_succeeded.is_set()

                bonus_online = True
                await asyncio.wait_for(refund_succeeded.wait(), timeout=1)
                await asyncio.wait_for(main.app.state.retry_queue.join(), timeout=1)

    asyncio.run(scenario())
