import logging
from logging import Logger
from typing import Any

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)

logger: Logger = logging.getLogger(name=__name__)


class MongoService:
    def __init__(self, uri: str, db_name: str) -> None:
        self.client: AsyncIOMotorClient[Any] = AsyncIOMotorClient(
            host = uri
        )
        self.db: AsyncIOMotorDatabase[Any] = self.client[
            db_name
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

    async def get_prediction(self, address: str):
        result = await self.pred_col.find_one({"address": address})
        return result
    
    async def record_prediction(self, address: str, label: str):
        _ = self.pred_col.update_one(
            filter={'address': address},
            update={"$set": {"address": address, "label": label}},
            upsert=True
        )
        logger.info(msg=f"Đã lưu kết quả dự đoán cho địa chỉ {address} vào DB.")