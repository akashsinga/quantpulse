from .ohlcv_fetcher import OHLCVFetcher
from .dhan_api_client import DhanAPIClient
from .exchange_mapper import ExchangeMapper

# Version information
__version__ = "1.0.0"


# Convenience factory function
def create_ohlcv_fetcher():
    """Create and configure a new OHLCVFetcher instance."""
    client = DhanAPIClient()
    mapper = ExchangeMapper()
    # Repository is created inside the fetcher
    return OHLCVFetcher(client, mapper)
