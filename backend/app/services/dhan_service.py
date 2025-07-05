# backend/app/services/dhan_service.py
"""
Service to handle Dhan related operations from Securities Import to OHLCV Import.
"""

import pandas as pd
import requests
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Tuple
from dhanhq import dhanhq

from app.utils.logger import get_logger
from app.core.config import settings
from app.core.exceptions import ExternalAPIError, ValidationError
from app.utils.enum import SecurityType, SettlementType, SecuritySegment

logger = get_logger(__name__)


class DhanService:
    """Service to perform all operations related to Dhan"""

    def __init__(self):
        """Initialize Dhan service with credentials validation"""
        if not settings.external.CLIENT_ID or not settings.external.ACCESS_TOKEN:
            raise ValidationError("Dhan API credentials not configured", details={"missing_fields": ["CLIENT_ID", "ACCESS_TOKEN"]})

        try:
            self.dhan_context = dhanhq(settings.external.CLIENT_ID, settings.external.ACCESS_TOKEN)
            logger.info("Dhan service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Dhan service: {e}")
            raise ExternalAPIError("Dhan", f"Initialization failed: {str(e)}")

    def test_connection(self) -> Dict[str, Any]:
        """Test Dhan API connection"""
        try:
            response = self.dhan_context.get_fund_limits()
            logger.info("Dhan API connection test successful")
            return {"status": "success", "message": "Connection established", "data": response}
        except Exception as e:
            logger.error(f"Dhan API connection test failed: {e}")
            raise ExternalAPIError("Dhan", f"Connection test failed: {str(e)}")

    def download_securities_master_detailed(self) -> pd.DataFrame:
        """Download detailed securities master using dhanhq library"""
        try:
            logger.info("Downloading securities data using dhanhq library")
            raw_data = self.dhan_context.fetch_security_list(mode="detailed")

            if raw_data is None or len(raw_data) == 0:
                raise ExternalAPIError("Dhan", "No data received from securities master API")

            # Convert to DataFrame
            df = pd.DataFrame(raw_data)
            logger.info(f"Downloaded {len(df)} total records from Dhan API")

            return df

        except Exception as e:
            logger.error(f"Failed to fetch securities master: {e}")
            raise ExternalAPIError("Dhan", f"Securities download failed: {str(e)}")

    def filter_securities_and_futures(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Filter and separate securities and futures from raw Dhan data"""
        logger.info(f"Filtering {len(df)} total records for NSE securities and futures")

        # Filter for NSE exchange only
        nse_df = df[df["EXCH_ID"] == "NSE"]

        if len(nse_df) == 0:
            logger.warning("No NSE securities found in the data")
            return pd.DataFrame(), pd.DataFrame()

        # Filter main securities (stocks and indices)
        stocks_df = nse_df[(nse_df["SEGMENT"] == "E") & (nse_df["INSTRUMENT_TYPE"] == "ES")]

        indices_df = nse_df[(nse_df["SEGMENT"] == "I") & (nse_df["INSTRUMENT_TYPE"] == "INDEX")]

        # Combine stocks and indices
        securities_df = pd.concat([stocks_df, indices_df], ignore_index=True)

        # Filter futures
        futures_df = nse_df[(nse_df["SEGMENT"] == "D") & (nse_df["INSTRUMENT"].isin(["FUTSTK", "FUTIDX"]))]

        logger.info(f"Filtered {len(stocks_df)} stocks, {len(indices_df)} indices, {len(futures_df)} futures")
        return securities_df, futures_df

    def process_securities_data(self, securities_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Process securities DataFrame into standardized format for database insertion"""
        processed_securities = []

        for index, row in securities_df.iterrows():
            try:
                # Validate row first
                if not self._validate_security_row(row):
                    continue

                security_data = {
                    'symbol': row["UNDERLYING_SYMBOL"].strip(),
                    'name': row["DISPLAY_NAME"].strip(),
                    'external_id': int(row["SECURITY_ID"]),
                    'security_type': self._map_security_type(row),
                    'segment': "EQUITY" if row["SEGMENT"] == "E" else "INDEX",
                    'isin': row.get("ISIN", "").strip() if row.get("ISIN") != "NA" else None,
                    'sector': None,  # Will be enriched later
                    'industry': None,  # Will be enriched later
                    'lot_size': self._safe_int(row.get("LOT_SIZE"), 1),
                    'tick_size': str(row.get("TICK_SIZE", "0.05")),
                    'is_active': True,
                    'is_tradeable': True,
                    'is_derivatives_eligible': self._is_derivatives_eligible(row),
                    'has_options': False,
                    'has_futures': False,
                }

                processed_securities.append(security_data)

            except Exception as e:
                logger.warning(f"Error processing security {row.get('UNDERLYING_SYMBOL', 'unknown')}: {e}")
                continue

        logger.info(f"Processed {len(processed_securities)} securities from {len(securities_df)} records")
        return processed_securities

    def process_futures_data(self, futures_df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Process futures DataFrame into standardized format
        Returns: (derivative_securities, futures_data)
        """
        derivative_securities = []
        futures_data = []

        for index, row in futures_df.iterrows():
            try:
                # Validate row first
                if not self._validate_futures_row(row):
                    continue

                # Create derivative security data
                derivative_security = {
                    'symbol': row["SYMBOL_NAME"].strip(),
                    'name': row["DISPLAY_NAME"].strip(),
                    'external_id': int(row["SECURITY_ID"]),
                    'security_type': SecurityType.DERIVATIVE.value,
                    'segment': row['INSTRUMENT'],
                    'isin': None,
                    'lot_size': self._safe_int(row.get("LOT_SIZE"), 1),
                    'tick_size': str(row.get("TICK_SIZE", "0.05")),
                    'is_active': True,
                    'is_tradeable': True,
                    'is_derivatives_eligible': False,
                    'has_options': False,
                    'has_futures': True,
                }
                derivative_securities.append(derivative_security)

                # Create futures relationship data
                future_data = {
                    'external_id': int(row["SECURITY_ID"]),
                    'underlying_security_id': int(row["UNDERLYING_SECURITY_ID"]) if row.get("UNDERLYING_SECURITY_ID") != "NA" else None,
                    'underlying_symbol': row["UNDERLYING_SYMBOL"].strip(),
                    'expiration_date': self._parse_expiry_from_symbol(row["SYMBOL_NAME"]),
                    'contract_month': self._extract_contract_month(row["SYMBOL_NAME"]),
                    'settlement_type': SettlementType.CASH.value if row["INSTRUMENT"] == "FUTIDX" else SettlementType.PHYSICAL.value,
                    'contract_size': 1.0,
                    'is_active': True,
                }
                futures_data.append(future_data)

            except Exception as e:
                logger.warning(f"Error processing future {row.get('DISPLAY_NAME', 'unknown')}: {e}")
                continue

        logger.info(f"Processed {len(derivative_securities)} derivative securities and {len(futures_data)} futures relationships")
        return derivative_securities, futures_data

    def validate_and_clean_data(self, securities_df: pd.DataFrame, futures_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Validate and clean both securities and futures data"""

        # Clean securities
        valid_securities = []
        for index, row in securities_df.iterrows():
            if self._validate_security_row(row):
                valid_securities.append(row)

        # Clean futures
        valid_futures = []
        for index, row in futures_df.iterrows():
            if self._validate_futures_row(row):
                valid_futures.append(row)

        clean_securities_df = pd.DataFrame(valid_securities)
        clean_futures_df = pd.DataFrame(valid_futures)

        logger.info(f"Validation complete: {len(clean_securities_df)} valid securities, {len(clean_futures_df)} valid futures")
        return clean_securities_df, clean_futures_df

    def fetch_sector_info(self, symbol: str = None, batch_symbols: List[str] = None) -> Dict[str, Any]:
        """Fetch sector and industry information for symbols (bulk request)"""
        try:
            url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"

            if batch_symbols:
                return self._fetch_sector_info_bulk(url, batch_symbols)
            elif symbol:
                return self._fetch_sector_info_bulk(url, [symbol])
            else:
                raise ValidationError("No symbols provided for sector info fetch")
        except Exception as e:
            logger.error(f"Error fetching sector information: {e}")
            raise ExternalAPIError("Dhan", f"Sector info fetch failed: {str(e)}")

    def _fetch_sector_info_bulk(self, url: str, symbols: List[str]) -> Dict[str, Any]:
        """Send one POST request for multiple symbols (comma-separated)"""
        try:
            symbol_string = ",".join(symbols)
            payload = {"data": {"fields": ["Sector", "SubSector"], "params": [{"field": "Exch", "op": "", "val": "NSE"}, {"field": "Sym", "op": "", "val": symbol_string}]}}

            logger.info(f"[Sector Enrichment] Requesting sector info for {len(symbols)} symbols: {symbol_string}")
            # logger.debug(f"[Sector Enrichment] Payload: {payload}")

            response = requests.post(url, json=payload, timeout=60)
            response.raise_for_status()

            data = response.json()
            # logger.debug(f"[Sector Enrichment] Response: {data}")

            if data.get("code") != 0 or "data" not in data:
                raise ExternalAPIError("Dhan", "Invalid response for sector info")

            results = {}
            for item in data["data"]:
                sym = item.get("Seosym", "").upper().replace("-", "").replace("LTD", "")
                results[sym] = {
                    "sector": item.get("Sector", "").strip(),
                    "industry": item.get("SubSector", "").strip(),
                    "symbol": item.get("DispSym", "").strip(),
                    "isin": item.get("Isin", "").strip(),
                }

            logger.info(f"[Sector Enrichment] Received sector info for {len(results)} of {len(symbols)} symbols")
            return results

        except Exception as e:
            logger.warning(f"[Sector Enrichment] Error in bulk sector info fetch for symbols {symbols}: {e}")
            return {}

    def enrich_securities_with_sector_info(self, securities_data: List[Dict[str, Any]], batch_size: int = 15) -> List[Dict[str, Any]]:
        """Enrich securities data with sector and industry information"""
        logger.info(f"Enriching {len(securities_data)} securities with sector information")

        enriched_securities = []

        for i in range(0, len(securities_data), batch_size):
            batch = securities_data[i:i + batch_size]
            batch_symbols = [sec['symbol'] for sec in batch]

            try:
                sector_results = self.fetch_sector_info(None, batch_symbols)

                for security in batch:
                    symbol = security['symbol']
                    sector_info = sector_results.get(symbol)

                    if sector_info:
                        security['sector'] = sector_info.get('sector')
                        security['industry'] = sector_info.get('industry')
                        if not security.get('isin') and sector_info.get('isin'):
                            security['isin'] = sector_info.get('isin')

                    enriched_securities.append(security)

                # Rate limiting between batches
                if i + batch_size < len(securities_data):
                    import time
                    time.sleep(0.5)

            except Exception as e:
                logger.warning(f"Error enriching batch {i//batch_size + 1}: {e}")
                enriched_securities.extend(batch)
                continue

        enriched_count = sum(1 for sec in enriched_securities if sec.get('sector'))
        logger.info(f"Successfully enriched {enriched_count}/{len(securities_data)} securities with sector information")

        return enriched_securities

    def get_statistics(self, securities_df: pd.DataFrame, futures_df: pd.DataFrame) -> Dict[str, Any]:
        """Get statistics about the processed data"""
        stats = {'total_securities': len(securities_df), 'total_futures': len(futures_df), 'securities_by_segment': {}, 'futures_by_type': {}}

        if len(securities_df) > 0:
            stats['securities_by_segment'] = securities_df['SEGMENT'].value_counts().to_dict()

        if len(futures_df) > 0:
            stats['futures_by_type'] = futures_df['INSTRUMENT'].value_counts().to_dict()

        return stats

    # Private helper methods
    def _validate_security_row(self, row: Dict) -> bool:
        """Validate required fields for security"""
        required_fields = ["SECURITY_ID", "UNDERLYING_SYMBOL", "EXCH_ID"]

        for field in required_fields:
            if not row.get(field) or pd.isna(row.get(field)):
                return False

        try:
            int(row["SECURITY_ID"])
        except (ValueError, TypeError):
            return False

        return True

    def _validate_futures_row(self, row: Dict) -> bool:
        """Validate required fields for futures"""
        if not self._validate_security_row(row):
            return False

        if not row.get("UNDERLYING_SECURITY_ID") or row.get("UNDERLYING_SECURITY_ID") == "NA":
            return False

        if not row.get("SYMBOL_NAME") or pd.isna(row.get("SYMBOL_NAME")):
            return False

        return True

    def _map_security_type(self, row: Dict) -> str:
        """Map Dhan security segment to our security type"""
        if row["SEGMENT"] == "E":
            return SecurityType.STOCK.value
        elif row["SEGMENT"] == "I":
            return SecurityType.INDEX.value
        elif row["SEGMENT"] == "D":
            return SecurityType.DERIVATIVE.value
        else:
            return SecurityType.STOCK.value

    def _safe_int(self, value, default: int = 1) -> int:
        """Safely convert value to integer"""
        try:
            if pd.isna(value):
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def _is_derivatives_eligible(self, row: Dict) -> bool:
        """Check if security is eligible for derivatives trading"""
        return (row.get("SEGMENT", "").upper() == "E" and self._safe_int(row.get("LOT_SIZE", 1)) >= 1)

    def _parse_expiry_from_symbol(self, symbol: str) -> Optional[date]:
        """Extract expiry date from symbol like 'ADANIENSOL-Sep2025-FUT'"""
        try:
            if "-" in symbol:
                parts = symbol.split("-")
                if len(parts) >= 2:
                    month_year = parts[1]  # "Sep2025"

                    import re
                    match = re.match(r'([A-Za-z]{3})(\d{4})', month_year)
                    if match:
                        month_str, year_str = match.groups()

                        month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

                        month_num = month_map.get(month_str)
                        if month_num:
                            year = int(year_str)
                            import calendar
                            last_day = calendar.monthrange(year, month_num)[1]
                            return date(year, month_num, last_day)

            return None
        except Exception as e:
            logger.warning(f"Error parsing expiry from symbol {symbol}: {e}")
            return None

    def _extract_contract_month(self, symbol: str) -> str:
        """Extract contract month from symbol"""
        try:
            if "-" in symbol:
                parts = symbol.split("-")
                if len(parts) >= 2:
                    month_year = parts[1]
                    import re
                    match = re.match(r'([A-Za-z]{3})', month_year)
                    if match:
                        return match.group(1).upper()
            return "UNK"
        except Exception:
            return "UNK"
