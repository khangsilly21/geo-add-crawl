import asyncio
import logging
import os
from logging import Logger
from typing import Any

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorCollection

from clients.http import HTTPClient
from clients.services import ServiceClient
from services.chainbase_crawler_service import (
    ChainbaseCrawlerService,
    TokenMeta,
    TransactionMeta,
)
from services.htpp_service import HttpService
from services.mongo_service import MongoService

_ = load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger: Logger = logging.getLogger(name=__name__)
API_KEYS: list[str] = os.getenv("API_KEYS", "").split(sep=",")


async def fetch_data(
    api_key: str, address: str, chainbase_crawler_client: ChainbaseCrawlerService
) -> dict[str, str | list[str] | list[dict[str, str]]] | dict[str, str | list[Any]]:
    try:
        logger.info(
            msg=f"Fetching data for address={address} using api_key={api_key[:6]}..."
        )
        tokenMeta_list: list[TokenMeta] = await chainbase_crawler_client.get_tokens(
            api_key=api_key, address=address
        )
        token_list: list[str] = [token.contract_address for token in tokenMeta_list]

        txMeta_list: list[
            TransactionMeta
        ] = await chainbase_crawler_client.get_transactions(
            api_key=api_key, address=address
        )
        tx_list: list[dict[str, str]] = [
            {"value": tx.value, "block_timestamp": tx.block_timestamp}
            for tx in txMeta_list
        ]
        
        balance :str = await chainbase_crawler_client.get_balance(
            api_key=api_key, address=address, chain_id=1)
        
        return {"address": address, "tokens": token_list, "transactions": tx_list, "balance": balance}
    except Exception as e:
        logger.error(msg=f"Error fetching data for address={address}: {e}")
        return {"address": address, "tokens": [], "transactions": []}


async def process_batch(
    addresses: list[str],
    chainbase_crawler_client: ChainbaseCrawlerService,
    mongo_client: MongoService,
) -> None:
    if len(addresses) > len(API_KEYS):
        raise ValueError("Number of addresses exceeds number of API keys.")

    tasks: list[Any] = []
    for i, address in enumerate(addresses):
        api_key: str = API_KEYS[i]
        tasks.append(
            fetch_data(
                api_key=api_key,
                address=address,
                chainbase_crawler_client=chainbase_crawler_client,
            )
        )
    results: list[dict[str, Any]] = await asyncio.gather(*tasks)
    address_collection: AsyncIOMotorCollection[Any] = mongo_client.get_collection(
        collection_name="user_data_v3"
    )
    try:
        if results:
            insert_result = await address_collection.insert_many(documents=results)
            logger.info(
                msg=f"Inserted {len(insert_result.inserted_ids)} documents into MongoDB"
            )
        else:
            logger.warning("No results to insert into MongoDB")
    except Exception as e:
        logger.error(f"Error inserting documents into MongoDB: {e}", exc_info=True)


async def main() -> None:
    BATCH_SIZE: int = len(API_KEYS)
    http_client: HttpService = HTTPClient.get_http_service()
    await http_client.start()
    logger.info(msg="HTTP client started")
    chainbase_crawler_client: ChainbaseCrawlerService = (
        ServiceClient.get_chainbase_crawler_service(api_keys=API_KEYS)
    )
    mongo_client: MongoService = ServiceClient.get_mongo_service()
    with open(file="data/user.txt", mode="r") as f:
        addresses: list[str] = [line.strip() for line in f.readlines() if line.strip()]

    batch_addresses: list[list[str]] = [
        addresses[i : i + BATCH_SIZE] for i in range(0, len(addresses), BATCH_SIZE)
    ]
    for batch in batch_addresses:
        await process_batch(
            addresses=batch,
            chainbase_crawler_client=chainbase_crawler_client,
            mongo_client=mongo_client,
        )
    await http_client.close()
    logger.info("HTTP client closed")


if __name__ == "__main__":
    asyncio.run(main())
