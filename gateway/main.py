import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from resilience import CircuitBreaker, REQUEST_TIMEOUT, ServiceUnavailable, require_success, send_request


FLIGHT_URL = os.getenv("FLIGHT_SERVICE_URL", "http://localhost:8060")
TICKET_URL = os.getenv("TICKET_SERVICE_URL", "http://localhost:8070")
BONUS_URL = os.getenv("BONUS_SERVICE_URL", "http://localhost:8050")

FLIGHT_SERVICE = "Flight Service"
TICKET_SERVICE = "Ticket Service"
BONUS_SERVICE = "Bonus Service"
RETRY_INTERVAL = 10.0

flight_breaker = CircuitBreaker(FLIGHT_SERVICE)
ticket_breaker = CircuitBreaker(TICKET_SERVICE)
bonus_breaker = CircuitBreaker(BONUS_SERVICE)
logger = logging.getLogger(__name__)


@dataclass
class PendingDelete:
    service: str
    url: str
    username: str
    retry_at: float = 0.0


async def send_delete(client: httpx.AsyncClient, job: PendingDelete):
    response = await send_request(
        job.service, client.delete(job.url, headers={"X-User-Name": job.username})
    )
    require_success(response)


def enqueue_retry(queue: asyncio.Queue[PendingDelete], job: PendingDelete):
    job.retry_at = time.monotonic() + RETRY_INTERVAL
    queue.put_nowait(job)


async def process_retries(queue: asyncio.Queue[PendingDelete]):
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        while True:
            job = await queue.get()
            try:
                await asyncio.sleep(max(0, job.retry_at - time.monotonic()))
                await send_delete(client, job)
            except ServiceUnavailable:
                enqueue_retry(queue, job)
            except HTTPException as exc:
                logger.error("Deferred DELETE %s rejected with HTTP %s", job.url, exc.status_code)
            finally:
                queue.task_done()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.retry_queue = asyncio.Queue()
    worker = asyncio.create_task(process_retries(app.state.retry_queue))
    try:
        yield
    finally:
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker


app = FastAPI(title="Gateway Service", lifespan=lifespan)


@app.exception_handler(ServiceUnavailable)
async def unavailable_response(request: Request, exc: ServiceUnavailable):
    # По postman надо 503, поэтому делаю так
    # А вообще в README по-другому..
    status_code = 503 if exc.service == BONUS_SERVICE else 500
    return JSONResponse(status_code=status_code, content={"message": str(exc)})


@app.get("/manage/health")
def health():
    return {"status": "OK"}


@app.get("/api/v1/flights")
async def get_flights(page: int = 1, size: int = 10):
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await flight_breaker.get(
            client, f"{FLIGHT_URL}/api/v1/flights", params={"page": page, "size": size}
        )
        require_success(response)
        return response.json()


async def read_privilege(client: httpx.AsyncClient, username: str):
    response = await bonus_breaker.get(
        client, f"{BONUS_URL}/api/v1/privilege", headers={"X-User-Name": username}
    )
    require_success(response)
    return response.json()


@app.get("/api/v1/privilege")
async def get_privilege(x_user_name: str = Header(..., alias="X-User-Name")):
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        return await read_privilege(client, x_user_name)


def ticket_details(ticket, flight):
    return {
        "ticketUid": ticket["ticketUid"],
        "flightNumber": ticket["flightNumber"],
        "fromAirport": flight.get("fromAirport", ""),
        "toAirport": flight.get("toAirport", ""),
        "date": flight.get("date", ""),
        "price": ticket["price"],
        "status": ticket["status"],
    }


async def enrich_ticket(client: httpx.AsyncClient, ticket):
    try:
        flight_response = await flight_breaker.get(
            client, f"{FLIGHT_URL}/api/v1/flights/{ticket['flightNumber']}"
        )
        flight = flight_response.json() if flight_response.status_code == 200 else {}
    except ServiceUnavailable:
        flight = {}
    return ticket_details(ticket, flight)


async def read_tickets(client: httpx.AsyncClient, username: str):
    response = await ticket_breaker.get(
        client, f"{TICKET_URL}/api/v1/tickets", headers={"X-User-Name": username}
    )
    require_success(response)
    return [await enrich_ticket(client, ticket) for ticket in response.json()]


@app.get("/api/v1/tickets")
async def get_tickets(x_user_name: str = Header(..., alias="X-User-Name")):
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        return await read_tickets(client, x_user_name)


@app.get("/api/v1/tickets/{ticket_uid}")
async def get_ticket(ticket_uid: str, x_user_name: str = Header(..., alias="X-User-Name")):
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await ticket_breaker.get(
            client, f"{TICKET_URL}/api/v1/tickets/{ticket_uid}", headers={"X-User-Name": x_user_name}
        )
        require_success(response)
        return await enrich_ticket(client, response.json())


class TicketPurchaseRequest(BaseModel):
    flightNumber: str
    price: int
    paidFromBalance: bool


async def delete_or_enqueue(client: httpx.AsyncClient, queue: asyncio.Queue[PendingDelete], job: PendingDelete):
    try:
        await send_delete(client, job)
    except ServiceUnavailable:
        enqueue_retry(queue, job)


@app.post("/api/v1/tickets")
async def purchase_ticket(
    data: TicketPurchaseRequest,
    request: Request,
    x_user_name: str = Header(..., alias="X-User-Name"),
):
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        flight_response = await flight_breaker.get(
            client, f"{FLIGHT_URL}/api/v1/flights/{data.flightNumber}"
        )
        if flight_response.status_code == 404:
            raise HTTPException(status_code=400, detail="Flight not found")
        require_success(flight_response)
        flight = flight_response.json()

        ticket_uid = str(uuid.uuid4())
        rollback = PendingDelete(TICKET_SERVICE, f"{TICKET_URL}/internal/tickets/{ticket_uid}", x_user_name)
        try:
            ticket_response = await send_request(
                TICKET_SERVICE,
                client.post(
                    f"{TICKET_URL}/api/v1/tickets",
                    json={
                        "flightNumber": data.flightNumber,
                        "price": data.price,
                        "username": x_user_name,
                        "ticketUid": ticket_uid,
                    },
                ),
            )
        except ServiceUnavailable as exc:
            # The timed-out request may still be inserting the ticket.
            if exc.request_may_have_run:
                enqueue_retry(request.app.state.retry_queue, rollback)
            raise
        require_success(ticket_response)

        try:
            bonus_response = await send_request(
                BONUS_SERVICE,
                client.post(
                    f"{BONUS_URL}/api/v1/privilege/use",
                    json={
                        "ticketUid": ticket_uid,
                        "price": data.price,
                        "paidFromBalance": data.paidFromBalance,
                    },
                    headers={"X-User-Name": x_user_name},
                ),
            )
            require_success(bonus_response)
        except (ServiceUnavailable, HTTPException) as exc:
            await delete_or_enqueue(client, request.app.state.retry_queue, rollback)
            if isinstance(exc, ServiceUnavailable) and exc.request_may_have_run:
                # Give an in-flight bonus transaction time to settle before undoing it.
                enqueue_retry(
                    request.app.state.retry_queue,
                    PendingDelete(BONUS_SERVICE, f"{BONUS_URL}/api/v1/privilege/{ticket_uid}", x_user_name),
                )
            raise

        bonus = bonus_response.json()
        return {
            **ticket_details(ticket_response.json(), flight),
            "paidByMoney": bonus["paidByMoney"],
            "paidByBonuses": bonus["paidByBonuses"],
            "privilege": {"balance": bonus["balance"], "status": bonus["status"]},
        }


@app.delete("/api/v1/tickets/{ticket_uid}")
async def cancel_ticket(
    ticket_uid: str, request: Request, x_user_name: str = Header(..., alias="X-User-Name")
):
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await send_request(
            TICKET_SERVICE,
            client.delete(f"{TICKET_URL}/api/v1/tickets/{ticket_uid}", headers={"X-User-Name": x_user_name}),
        )
        require_success(response)
        await delete_or_enqueue(
            client,
            request.app.state.retry_queue,
            PendingDelete(BONUS_SERVICE, f"{BONUS_URL}/api/v1/privilege/{ticket_uid}", x_user_name),
        )
        return Response(status_code=204)


@app.get("/api/v1/me")
async def get_me(x_user_name: str = Header(..., alias="X-User-Name")):
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        tickets = await read_tickets(client, x_user_name)
        try:
            bonus = await read_privilege(client, x_user_name)
            privilege = {"balance": bonus["balance"], "status": bonus["status"]}
        except ServiceUnavailable:
            privilege = {}
        return {"tickets": tickets, "privilege": privilege}
