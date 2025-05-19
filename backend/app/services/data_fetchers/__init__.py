# app/services/data_fetchers/__init__.py

from .ohlcv_fetcher import OHLCVFetcher
from .dhan_api_client import DhanAPIClient
from .exchange_mapper import ExchangeMapper

# Version information
__version__ = "1.0.0"


def create_ohlcv_fetcher():
    """Create and configure a new OHLCVFetcher instance with all session protections."""
    # Create the API client
    client = DhanAPIClient()

    # Create the mapper with caching
    mapper = ExchangeMapper()

    # Create the repository
    from ..repositories.ohlcv_repository import OHLCVRepository

    repository = OHLCVRepository()

    # Return configured fetcher
    return OHLCVFetcher(client, mapper, repository)
