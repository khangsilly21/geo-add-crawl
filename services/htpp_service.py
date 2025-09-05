import asyncio
import logging
from typing import Any

import aiohttp

from services.base_singleton import SingletonMeta

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10  # Default timeout for HTTP requests in seconds
MAX_RETRIES = 3  # Maximum number of retries


class HTTPStatus:
    OK: int = 200
    CREATED: int = 201
    BAD_REQUEST: int = 400
    NOT_FOUND: int = 404
    INTERNAL_SERVER_ERROR: int = 500


class HttpService(metaclass=SingletonMeta):
    _is_initialized: bool = False

    def __init__(self) -> None:
        if not HttpService._is_initialized:
            logger.info(msg="HTTP Client initialized but not started yet")
            HttpService._is_initialized = True
            self.session: aiohttp.ClientSession | None = None
        else:
            logger.info(
                "HttpService instance already initialized, skipping re-initialization"
            )

    async def start(self, timeout: float = DEFAULT_TIMEOUT) -> None:
        """
        Start the HTTP client session.
        Args:
            timeout: Timeout for the HTTP requests in seconds
        """
        if self.session is None:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=timeout)
            )
            logger.info(msg="AIOHTTP session started")
        else:
            logger.info("AIOHTTP session is already started")

    async def close(self) -> None:
        if self.session:
            await self.session.close()

    @staticmethod
    def _normalize_params(params: dict[str, Any]) -> dict[str, Any]:
        return {
            k: str(v).lower() if isinstance(v, bool) else v for k, v in params.items()
        }

    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        """
        Send a GET request and return JSON response with auto-retry.
        Args:
            url: The URL to send the request to
            params: Query parameters
            headers: Request headers
        Returns:
            JSON response data or None if error occurred
        """
        data: dict[str, Any] | list[dict[str, Any]] | None = None
        params = (
            self._normalize_params(params)
            if isinstance(params, dict) and params
            else {}
        )
        if not self.session:
            logger.error(
                msg="AIOHTTP session should be created before used via start() method"
            )
            raise RuntimeError("AIOHTTP session has not been initialized")

        for attempt in range(MAX_RETRIES + 1):
            try:
                async with self.session.get(
                    url=url,
                    params=params,
                    headers=headers,
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return data

            except aiohttp.ClientResponseError as e:
                if e.status == HTTPStatus.NOT_FOUND:  # Don't retry for NOT FOUND errors
                    logger.info(
                        f"Resource not found ({HTTPStatus.NOT_FOUND}) for {url}, not retrying"
                    )
                    break

                if attempt < MAX_RETRIES:
                    wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
                    logger.warning(
                        f"HTTP error {e.status} for {url}, retrying in {wait_time}s (attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"HTTP error {e.status} for {url} after {MAX_RETRIES} retries"
                    )

            except aiohttp.ClientError as e:
                if attempt < MAX_RETRIES:
                    wait_time = 2**attempt
                    logger.warning(
                        f"Client error for {url}: {e}, retrying in {wait_time}s (attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"Client error for {url}: {e} after {MAX_RETRIES} retries"
                    )

            except Exception as e:
                if attempt < MAX_RETRIES:
                    wait_time = 2**attempt
                    logger.warning(
                        f"Unexpected error for {url}: {e}, retrying in {wait_time}s (attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"Unexpected error for {url}: {e} after {MAX_RETRIES} retries"
                    )

        return data
