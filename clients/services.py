from clients.http import HTTPClient
from services.chainbase_crawler_service import ChainbaseCrawlerService
from services.mongo_service import MongoService


class ServiceClient:
    _chainbase_crawler_service: ChainbaseCrawlerService | None = None
    _mongo_service: MongoService | None = None

    @classmethod
    def get_chainbase_crawler_service(
        cls, api_keys: list[str]
    ) -> ChainbaseCrawlerService:
        """
        Get an instance of ChainbaseCrawlerService.
        """
        if not cls._chainbase_crawler_service:
            http_service = HTTPClient.get_http_service()
            cls._chainbase_crawler_service = ChainbaseCrawlerService(
                api_keys=api_keys, http_client=http_service
            )
        return cls._chainbase_crawler_service

