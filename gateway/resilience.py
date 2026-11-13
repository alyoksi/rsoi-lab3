import asyncio
import time
from collections.abc import Awaitable

import httpx
from fastapi import HTTPException


REQUEST_TIMEOUT = 2.0


class ServiceUnavailable(Exception):
    def __init__(self, service: str):
        self.service = service
        super().__init__(f"{service} unavailable")

    @property
    def request_may_have_run(self) -> bool:
        return not isinstance(self.__cause__, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout))


async def send_request(service: str, request: Awaitable[httpx.Response]) -> httpx.Response:
    try:
        response = await request
    except httpx.RequestError as exc:
        raise ServiceUnavailable(service) from exc
    if response.status_code >= 500:
        raise ServiceUnavailable(service)
    return response


def require_success(response: httpx.Response) -> None:
    if 200 <= response.status_code < 300:
        return
    try:
        body = response.json()
    except ValueError:
        body = {}
    detail = body.get("detail", response.reason_phrase) if isinstance(body, dict) else response.reason_phrase
    raise HTTPException(status_code=response.status_code, detail=detail)


class CircuitBreaker:
    def __init__(self, service: str, failure_threshold: int = 3, recovery_timeout: float = 5.0):
        self.service = service
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.retry_at = None
        self.probe_in_progress = False
        self.generation = 0

    def _open(self):
        self.retry_at = time.monotonic() + self.recovery_timeout
        self.generation += 1

    async def get(self, client: httpx.AsyncClient, url: str, **kwargs) -> httpx.Response:
        if self.retry_at is not None:
            if time.monotonic() < self.retry_at or self.probe_in_progress:
                raise ServiceUnavailable(self.service)
            self.probe_in_progress = True

        is_probe = self.probe_in_progress
        generation = self.generation
        try:
            response = await send_request(self.service, client.get(url, **kwargs))
        except ServiceUnavailable:
            if generation == self.generation:
                self.failures += 1
                if is_probe or self.failures >= self.failure_threshold:
                    self._open()
            raise
        except asyncio.CancelledError:
            if is_probe:
                self._open()
            raise
        else:
            # Ignore completions of older calls after the circuit has opened.
            if generation == self.generation:
                self.failures = 0
                self.retry_at = None
            return response
        finally:
            if is_probe:
                self.probe_in_progress = False
