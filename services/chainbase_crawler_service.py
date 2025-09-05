import asyncio
import logging
from enum import Enum
from logging import Logger
from typing import Any

from pydantic import BaseModel, Field

from services.htpp_service import HttpService

logger: Logger = logging.getLogger(name=__name__)


class EndPoint(str, Enum):
    TOKEN = "/account/tokens"
    TRANSACTION = "/account/txs"


class ChainbaseAPIError(Exception):
    """Custom exception for Chainbase API errors"""

    pass


class TokenMeta(BaseModel):
    contract_address: str


class TransactionMeta(BaseModel):
    value: str
    block_timestamp: str


class ChainbaseResponse(BaseModel):
    code: int
    message: str
    data: Any | None
    next_page: int | None = None


class TokenListResponse(ChainbaseResponse):
    data: list[TokenMeta] | None = Field(
        default=[], description="List of tokens held by the address."
    )


class TransactionListResponse(ChainbaseResponse):
    data: list[TransactionMeta] | None = Field(
        default=[], description="List of transactions made by the address."
    )


class ChainbaseCrawlerService:
    def __init__(self, api_keys: list[str], http_client: HttpService) -> None:
        self.http_client: HttpService = http_client

        self.base_url: str = "https://api.chainbase.online/v1"

    async def _fetch_paginated(
        self,
        api_key: str,
        endpoint: EndPoint,
        chain_id: int = 1,
        address: str = "",
        delay: float = 0.5,
    ) -> list[dict[str, Any]]:
        """Generic function to fetch paginated data from Chainbase API"""
        results: list[dict[str, Any]] = []

        FROM_TS: int = 1704067200
        END_TS: int = 1711929600
        MAX_PAGE: int = 100  # Get max 10,000 items
        params: dict[str, Any] = {
            "chain_id": chain_id,
            "address": address,
            "page": 1,
            "limit": 100,
        }
        if endpoint == EndPoint.TRANSACTION:
            params["from_timestamp"] = FROM_TS
            params["end_timestamp"] = END_TS

        for _ in range(MAX_PAGE):
            res: (
                dict[str, Any] | list[dict[str, Any]] | None
            ) = await self.http_client.get(
                url=self.base_url + endpoint,
                params=params,
                headers={"x-api-key": api_key},
            )
            await asyncio.sleep(delay)
            if res is None:
                logger.warning(msg="Could not received response from Chainbase API")
                return []

            if not isinstance(res, dict):
                logger.error(msg=f"Unexpected response: {res}")
                raise ChainbaseAPIError(f"Unexpected response: {res}")

            cb_res: ChainbaseResponse = ChainbaseResponse(**res)
            if cb_res.code != 0:
                logger.warning(msg=f"Chainbase API Error: {res}")
                raise ChainbaseAPIError(f"Chainbase API Error: {res}")

            results.extend(res.get("data") or [])

            if cb_res.next_page is None:
                break

            params["page"] += 1
        if not results or len(results) == 0:
            logger.warning(
                msg=f"No data found for {endpoint} with address {address} on chain {chain_id}"
            )
        else:
            logger.info(
                msg=f"Fetched {len(results)} items from {endpoint} for address {address} on chain {chain_id}"
            )
        return results

    async def get_transactions(
        self, api_key: str, address: str, chain_id: int = 1
    ) -> list[TransactionMeta]:
        txs: list[dict[str, Any]] = await self._fetch_paginated(
            endpoint=EndPoint.TRANSACTION,
            chain_id=chain_id,
            address=address,
            api_key=api_key,
        )
        list_txs: list[TransactionMeta] = [TransactionMeta(**tx) for tx in txs]
        return list_txs

    async def get_tokens(
        self, api_key: str, address: str, chain_id: int = 1
    ) -> list[TokenMeta]:
        tokens: list[dict[str, Any]] = await self._fetch_paginated(
            endpoint=EndPoint.TOKEN,
            chain_id=chain_id,
            address=address,
            api_key=api_key,
        )
        list_tokens: list[TokenMeta] = [TokenMeta(**token) for token in tokens]
        return list_tokens
