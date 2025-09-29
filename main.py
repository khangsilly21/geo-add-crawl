import argparse
import asyncio
import json
import logging
import os
from datetime import datetime
from logging import Logger
from typing import Any

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo.results import InsertManyResult

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
    api_key: str, address: str, chainbase_crawler_client: ChainbaseCrawlerService, chain_id: int = 1,
) -> dict[str, str | list[str] | list[dict[str, str]]] | dict[str, str | list[Any]]:
    try:
        logger.info(
            msg=f"Fetching data for address={address} using api_key={api_key[:6]}..."
        )
        tokenMeta_list: list[TokenMeta] = await chainbase_crawler_client.get_tokens(
            api_key=api_key, address=address, chain_id=chain_id
        )
        token_list: list[str] = [token.contract_address for token in tokenMeta_list]

        txMeta_list: list[
            TransactionMeta
        ] = await chainbase_crawler_client.get_transactions(
            api_key=api_key, address=address, chain_id=chain_id
        )
        tx_list: list[dict[str, str]] = [
            {"value": tx.value, "block_timestamp": tx.block_timestamp}
            for tx in txMeta_list
        ]
        
        balance :str = await chainbase_crawler_client.get_balance(
            api_key=api_key, address=address, chain_id=chain_id)
        
        return {"address": address, "tokens": token_list, "transactions": tx_list, "balance": balance}
    except Exception as e:
        logger.error(msg=f"Error fetching data for address={address}: {e}")
        return {"address": address, "tokens": [], "transactions": []}


async def process_batch(
    addresses: list[str],
    chainbase_crawler_client: ChainbaseCrawlerService,
    chain_id: int = 1,
) -> list[dict[str, Any]]:
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
                chain_id=chain_id,
            )
        )
    results: list[dict[str, Any]] = await asyncio.gather(*tasks)
    return results


async def main(chain_id: int) -> None:
    BATCH_SIZE: int = len(API_KEYS)
    http_client: HttpService = HTTPClient.get_http_service()
    await http_client.start()
    logger.info(msg="HTTP client started")
    chainbase_crawler_client: ChainbaseCrawlerService = (
        ServiceClient.get_chainbase_crawler_service(api_keys=API_KEYS)
    )
    
    with open(file="data/test_user.txt", mode="r") as f:
        addresses: list[str] = [line.strip() for line in f.readlines() if line.strip()]

    batch_addresses: list[list[str]] = [
        addresses[i : i + BATCH_SIZE] for i in range(0, len(addresses), BATCH_SIZE)
    ]
    final_results: list[dict[str, Any]] = []
    for batch in batch_addresses:
        result: list[dict[str, Any]] = await process_batch(
            addresses=batch,
            chainbase_crawler_client=chainbase_crawler_client,
            chain_id=chain_id,
        )
        final_results.extend(result)
    
    MONGO_URI: str = os.getenv("MONGO_URI", "")
    if not MONGO_URI:
        logger.error("MONGO_URI is not set in environment variables.")
        return
    DB_NAME: str = os.getenv("DB_NAME", "geo_data")
    
    mongo_client: MongoService = MongoService(uri=MONGO_URI, db_name=DB_NAME)
    try:
        if final_results:
            address_collection: AsyncIOMotorCollection[Any] = mongo_client.get_collection(
                collection_name=f"user_data_v4_{chain_id}" 
            )
            logger.info("Starting to insert documents into MongoDB")
            insert_result: InsertManyResult = await address_collection.insert_many(
                documents=final_results, ordered=False
            )
            logger.info(
                msg=f"Inserted {len(insert_result.inserted_ids)} documents into MongoDB"
            )
        else:
            logger.warning(msg="No results to insert into MongoDB")
    except Exception as e:
        logger.error(msg=f"Error inserting documents into MongoDB: {e}", exc_info=True)

        # ghi backup ra file JSON
        backup_file: str = f"backup_results_{datetime.now().strftime(format='%Y%m%d_%H%M%S')}.json"
        try:
            with open(file=backup_file, mode = "w", encoding="utf-8") as f:
                json.dump(obj=final_results, fp = f, ensure_ascii=False, indent=2)
            logger.warning(msg=f"Saved backup results to {backup_file}")
        except Exception as file_err:
            logger.error(msg=f"Failed to write backup JSON: {file_err}", exc_info=True)
        
    await http_client.close()
    logger.info("HTTP client closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chainbase Crawler")
    _ = parser.add_argument(
        "--chain-id",
        type=int,
        default=1,
        help="Chain ID to fetch data from (default: 1)"
    )
    args = parser.parse_args()
    asyncio.run(main(chain_id=args.chain_id))
