from services.htpp_service import HttpService


class HTTPClient:
    _http_service: HttpService | None = None

    @classmethod
    def get_http_service(cls) -> HttpService:
        """
        Get an instance of HttpService.
        """
        if not cls._http_service:
            cls._http_service = HttpService()
        return cls._http_service

    @classmethod
    async def start(cls) -> None:
        """
        Start the HTTP client service.
        """
        if not cls._http_service:
            cls._http_service = HttpService()
        await cls._http_service.start()

    @classmethod
    async def close(cls) -> None:
        """
        Close the HTTP client service.
        """
        if cls._http_service:
            await cls._http_service.close()
