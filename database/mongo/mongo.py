from database.mongo.schema import Wallet

import beanie
from pymongo import AsyncMongoClient

async def init_wallet_collection(uri: str, db_name:str) -> None:
    """
    Initialize the Wallet collection in the database.
    """
    client = AsyncMongoClient(uri)
    db = client[db_name]
    await beanie.init_beanie(database=db, document_models=[Wallet])
    
    logger.info(msg="Wallet collection initialized successfully.")

async def insert