import logging
import os
from logging import Logger
from typing import Any

from dotenv import load_dotenv
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)

logger: Logger = logging.getLogger(name=__name__)
_ = load_dotenv()


class MongoService:
    def __init__(self) -> None:
        self.client: AsyncIOMotorClient[Any] = AsyncIOMotorClient(
            os.getenv("MONGO_URI")
        )
        self.db: AsyncIOMotorDatabase[Any] = self.client[
            os.getenv("DB_NAME", "geo_data")
        ]

    async def close(self) -> None:
        """
        Close the MongoDB client connection.
        """
        self.client.close()

    def get_collection(self, collection_name: str) -> AsyncIOMotorCollection[Any]:
        """
        Get a MongoDB collection by name.
        """
        return self.db[collection_name]
