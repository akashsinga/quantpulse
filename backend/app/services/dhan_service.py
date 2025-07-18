# backend/app/services/dhan_service.py
"""
Service to handle Dhan related operations from Securities Import to OHLCV Import.
Updated to work with actual Dhan CSV structure.
"""

import pandas as pd
import requests
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from dhanhq import dhanhq
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.utils.logger import get_logger
from app.core.config import settings
from app.core.exceptions import ExternalAPIError, ValidationError
from app.utils.enum import SecurityType, SettlementType, SecuritySegment, ExpiryMonth

logger = get_logger(__name__)


class DhanService:
    """Service to perform all operations related to Dhan"""

    def __init__(self):
        """Initialize Dhan service with credentials validation"""
        if not settings.external.CLIENT_ID or not settings.external.ACCESS_TOKEN:
            raise ValidationError("Dhan API credentials not configured", details={"missing_fields": ["CLIENT_ID", "ACCESS_TOKEN"]})

        try:
            self.dhan_context = dhanhq(settings.external.CLIENT_ID, settings.external.ACCESS_TOKEN)
            self._lock = threading.Lock()
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

    def filter_securities_and_futures(self, df: pd.DataFrame, supported_exchanges: List[str]) -> pd.DataFrame:
        """Filter securities from raw Dhan data for supported exchanges and relevant instrument types"""
        logger.info(f"Filtering {len(df)} total records for exchanges: {supported_exchanges}")

        # Filter for supported exchanges
        filtered_df = df[df["EXCH_ID"].isin(supported_exchanges)]

        if len(filtered_df) == 0:
            logger.warning(f"No securities found for supported exchanges: {supported_exchanges}")
            return pd.DataFrame()

        # Filter for relevant instrument types
        relevant_segments = ["D", "E", "I"]
        relevant_instruments = ["EQUITY", "INDEX", "FUTSTK", "FUTIDX"]
        relevant_instrument_types = ["ES", "INDEX", "FUT", "FUTIDX", "FUTSTK"]

        filtered_df = filtered_df[filtered_df["SEGMENT"].isin(relevant_segments)]
        filtered_df = filtered_df[filtered_df["INSTRUMENT"].isin(relevant_instruments)]
        final_df = filtered_df[filtered_df["INSTRUMENT_TYPE"].isin(relevant_instrument_types)]

        logger.info(f"Filtered {len(final_df)} records from {len(filtered_df)} exchange records using instrument types: {relevant_instruments}")
        return final_df

    def validate_and_clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean securities data"""
        valid_securities = []

        for index, row in df.iterrows():
            if self._validate_security_row(row):
                valid_securities.append(row)

        clean_df = pd.DataFrame(valid_securities)
        logger.info(f"Validation complete: {len(clean_df)} valid securities from {len(df)} records")
        return clean_df

    def process_securities_data(self, securities_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Process securities DataFrame into standardized format for database insertion"""

        # First pass: Build derivatives mapping
        derivatives_map = self._build_derivatives_mapping(securities_df)

        processed_securities = []

        for index, row in securities_df.iterrows():
            try:
                # Validate row first
                if not self._validate_security_row(row):
                    continue

                # Map instrument type to SecurityType enum
                security_type = self._map_security_type(row)

                # Use SYMBOL_NAME for derivatives, UNDERLYING_SYMBOL for equities/indices
                if self._is_derivative(security_type):
                    symbol = self._safe_strip(row.get("SYMBOL_NAME"))
                    underlying_symbol = self._safe_strip(row.get("UNDERLYING_SYMBOL"))
                else:
                    symbol = self._safe_strip(row.get("UNDERLYING_SYMBOL"))
                    underlying_symbol = symbol

                security_data = {
                    'symbol': symbol,
                    'name': self._safe_strip(row.get("DISPLAY_NAME")),
                    'external_id': int(row["SECURITY_ID"]),
                    'exchange_code': self._safe_strip(row.get("EXCH_ID")),
                    'security_type': security_type,
                    'segment': self._map_segment(row),
                    'isin': self._safe_strip(row.get("ISIN")) if row.get("ISIN") not in ["NA", "null", None] else None,
                    'sector': None,  # Will be enriched later
                    'industry': None,  # Will be enriched later
                    'lot_size': self._safe_int(row.get("LOT_SIZE"), 1),
                    'tick_size': str(row.get("TICK_SIZE", "0.05")),
                    'is_active': True,
                    'is_tradeable': True,
                }

                # Set derivative flags based on security type
                if self._is_derivative(security_type):
                    # Derivatives themselves don't have derivative flags
                    security_data.update({
                        'is_derivatives_eligible': False,
                        'has_options': False,
                        'has_futures': False,
                    })
                else:
                    # Only underlying securities get derivative flags from the mapping
                    derivative_flags = derivatives_map.get(underlying_symbol, {'has_futures': False, 'has_options': False, 'is_derivatives_eligible': False})
                    security_data.update(derivative_flags)

                # Add derivative-specific fields if it's a derivative
                if self._is_derivative(security_type):
                    derivative_data = self._extract_derivative_data(row)
                    security_data.update(derivative_data)

                processed_securities.append(security_data)

            except Exception as e:
                logger.warning(f"Error processing security {row.get('UNDERLYING_SYMBOL', 'unknown')}: {e}")
                continue

        logger.info(f"Processed {len(processed_securities)} securities from {len(securities_df)} records")
        return processed_securities

    def _build_derivatives_mapping(self, securities_df: pd.DataFrame) -> Dict[str, Dict[str, bool]]:
        """Build mapping of which underlyings have futures/options"""
        derivatives_map = {}

        for index, row in securities_df.iterrows():
            try:
                instrument = row.get("INSTRUMENT", "").upper()
                underlying_symbol = self._safe_strip(row.get("UNDERLYING_SYMBOL"))

                if not underlying_symbol:
                    continue

                # Initialize if not exists
                if underlying_symbol not in derivatives_map:
                    derivatives_map[underlying_symbol] = {'has_futures': False, 'has_options': False, 'is_derivatives_eligible': False}

                # Check for futures
                if instrument in ["FUTSTK", "FUTIDX", "FUTCOM", "FUTCUR"]:
                    derivatives_map[underlying_symbol]['has_futures'] = True

                # Check for options
                if instrument in ["OPTSTK", "OPTIDX", "OPTFUT", "OPTCUR"]:
                    derivatives_map[underlying_symbol]['has_options'] = True

            except Exception as e:
                logger.debug(f"Error processing derivatives mapping for row: {e}")
                continue

        # Set is_derivatives_eligible based on whether security has futures OR options
        for underlying_symbol, flags in derivatives_map.items():
            flags['is_derivatives_eligible'] = flags['has_futures'] or flags['has_options']

        eligible_count = sum(1 for flags in derivatives_map.values() if flags['is_derivatives_eligible'])
        futures_count = sum(1 for flags in derivatives_map.values() if flags['has_futures'])
        options_count = sum(1 for flags in derivatives_map.values() if flags['has_options'])

        logger.info(f"Derivatives mapping: {eligible_count} derivatives-eligible, {futures_count} with futures, {options_count} with options")

        return derivatives_map

    def enrich_securities_with_sector_info(self, securities_data: List[Dict[str, Any]], batch_size: int = 15, max_workers: int = 3) -> List[Dict[str, Any]]:
        """Enrich securities data with sector and industry information using parallel processing"""
        logger.info(f"Enriching {len(securities_data)} securities with sector information using {max_workers} workers")

        # Filter securities that have ISIN and are EQUITY type
        securities_with_isin = [sec for sec in securities_data if sec.get('isin') and sec.get('security_type') == SecurityType.EQUITY.value]

        if not securities_with_isin:
            logger.info("No equity securities with ISIN found, skipping sector enrichment")
            return securities_data

        logger.info(f"Found {len(securities_with_isin)} equity securities with ISIN for enrichment")

        # Group securities by exchange for parallel processing
        securities_by_exchange = {}
        for sec in securities_with_isin:
            exchange_code = sec.get('exchange_code', 'NSE')
            if exchange_code not in securities_by_exchange:
                securities_by_exchange[exchange_code] = []
            securities_by_exchange[exchange_code].append(sec)

        # Process each exchange in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_exchange = {}
            for exchange_code, exchange_securities in securities_by_exchange.items():
                future = executor.submit(self._enrich_exchange_securities, exchange_code, exchange_securities, batch_size)
                future_to_exchange[future] = exchange_code

            # Collect results from all exchanges
            for future in as_completed(future_to_exchange):
                exchange_code = future_to_exchange[future]
                try:
                    future.result()  # This modifies securities in place
                    logger.info(f"Completed sector enrichment for {exchange_code}")
                except Exception as e:
                    logger.warning(f"Error enriching exchange {exchange_code}: {e}")

        # Add back securities without ISIN (unchanged)
        securities_without_isin = [sec for sec in securities_data if not sec.get('isin') or sec.get('security_type') != SecurityType.EQUITY.value]
        enriched_securities = securities_with_isin + securities_without_isin

        enriched_count = sum(1 for sec in enriched_securities if sec.get('sector'))
        logger.info(f"Successfully enriched {enriched_count}/{len(securities_data)} securities with sector information")

        return enriched_securities

    def fetch_sector_info(self, symbol: str = None, batch_symbols: List[str] = None, exchange_code: str = "NSE") -> Dict[str, Any]:
        """Fetch sector and industry information for symbols (bulk request)"""
        try:
            url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"

            if batch_symbols:
                return self._fetch_sector_info_bulk(url, batch_symbols, exchange_code)
            elif symbol:
                return self._fetch_sector_info_bulk(url, [symbol], exchange_code)
            else:
                raise ValidationError("No symbols provided for sector info fetch")
        except Exception as e:
            logger.error(f"Error fetching sector information: {e}")
            raise ExternalAPIError("Dhan", f"Sector info fetch failed: {str(e)}")

    def get_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get statistics about the processed data"""
        stats = {'total_securities': len(df), 'securities_by_exchange': {}, 'securities_by_segment': {}, 'securities_by_instrument': {}}

        if len(df) > 0:
            stats['securities_by_exchange'] = df['EXCH_ID'].value_counts().to_dict()
            stats['securities_by_segment'] = df['SEGMENT'].value_counts().to_dict()
            stats['securities_by_instrument'] = df['INSTRUMENT'].value_counts().to_dict()

        return stats

    # Private helper methods
    def _validate_security_row(self, row: Dict) -> bool:
        """Validate required fields for security"""
        required_fields = ["SECURITY_ID", "UNDERLYING_SYMBOL", "EXCH_ID", "INSTRUMENT"]

        for field in required_fields:
            if not row.get(field) or pd.isna(row.get(field)):
                return False

        try:
            int(row["SECURITY_ID"])
        except (ValueError, TypeError):
            return False

        return True

    def _safe_strip(self, value, default: str = "") -> str:
        """Safely strip string values, handle floats and NaN"""
        try:
            if pd.isna(value) or value in ["NA", "null"]:
                return default
            return str(value).strip()
        except (AttributeError, TypeError):
            return default

    def _safe_int(self, value, default: int = 1) -> int:
        """Safely convert value to integer"""
        try:
            if pd.isna(value) or value in ["NA", "null"]:
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def _map_security_type(self, row: Dict) -> str:
        """Map Dhan instrument to SecurityType enum"""
        instrument = row.get("INSTRUMENT", "").upper()

        # Map to SecurityType enum values
        instrument_mapping = {
            "EQUITY": SecurityType.EQUITY.value,
            "INDEX": SecurityType.INDEX.value,
            "FUTSTK": SecurityType.FUTSTK.value,
            "FUTIDX": SecurityType.FUTIDX.value,
            "FUTCOM": SecurityType.FUTCOM.value,
            "FUTCUR": SecurityType.FUTCUR.value,
            "OPTSTK": SecurityType.OPTSTK.value,
            "OPTIDX": SecurityType.OPTIDX.value,
            "OPTFUT": SecurityType.OPTCOM.value,
            "OPTCUR": SecurityType.OPTCUR.value,
        }

        return instrument_mapping.get(instrument, SecurityType.EQUITY.value)

    def _map_segment(self, row: Dict) -> str:
        """Map Dhan segment to our segment classification"""
        segment = row.get("SEGMENT", "").upper()

        segment_mapping = {
            "E": SecuritySegment.EQUITY.value,
            "D": SecuritySegment.DERIVATIVE.value,
            "C": SecuritySegment.CURRENCY.value,
            "M": SecuritySegment.COMMODITY.value,
            "I": SecuritySegment.INDEX.value,
        }

        return segment_mapping.get(segment, SecuritySegment.EQUITY.value)

    def _is_derivative(self, security_type: str) -> bool:
        """Check if security type is a derivative"""
        derivative_types = [SecurityType.FUTSTK.value, SecurityType.FUTIDX.value, SecurityType.FUTCOM.value, SecurityType.FUTCUR.value, SecurityType.OPTSTK.value, SecurityType.OPTIDX.value, SecurityType.OPTCOM.value, SecurityType.OPTCUR.value]
        return security_type in derivative_types

    def _extract_derivative_data(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Extract derivative-specific data for futures and options"""
        derivative_data = {}

        # Get expiry date from SM_EXPIRY_DATE field
        expiry_date = self._parse_expiry_date(row.get("SM_EXPIRY_DATE"))
        if expiry_date:
            derivative_data['expiration_date'] = expiry_date
            derivative_data['contract_month'] = self._get_contract_month_from_date(expiry_date)
        else:
            derivative_data['expiration_date'] = None
            derivative_data['contract_month'] = 'UNK'

        # Extract underlying information
        derivative_data['underlying_symbol'] = self._safe_strip(row.get("UNDERLYING_SYMBOL"))
        underlying_security_id = row.get("UNDERLYING_SECURITY_ID")
        if underlying_security_id and underlying_security_id not in ["NA", "null"]:
            derivative_data['underlying_security_id'] = underlying_security_id
        else:
            derivative_data['underlying_security_id'] = None

        # Set settlement type based on instrument
        instrument = self._safe_strip(row.get("INSTRUMENT")).upper()
        if instrument in ["FUTIDX", "OPTIDX"]:
            derivative_data['settlement_type'] = SettlementType.CASH.value
        else:
            derivative_data['settlement_type'] = SettlementType.PHYSICAL.value

        # Add option-specific data if it's an option
        if instrument.startswith("OPT"):
            derivative_data['strike_price'] = self._safe_float(row.get("STRIKE_PRICE"))
            derivative_data['option_type'] = self._safe_strip(row.get("OPTION_TYPE"))

        return derivative_data

    def _parse_expiry_date(self, expiry_str: str) -> Optional[date]:
        """Parse expiry date using datetime.strptime for multiple formats"""
        if not expiry_str or expiry_str in ["NA", "null", "########"]:
            return None

        # List of possible date formats
        date_formats = [
            "%Y-%m-%d",  # 2024-08-28
            "%d-%m-%Y",  # 30-09-2025
            "%Y/%m/%d",  # 2024/08/28
            "%d/%m/%Y",  # 30/09/2025
        ]

        for date_format in date_formats:
            try:
                parsed_date = datetime.strptime(expiry_str.strip(), date_format)
                return parsed_date.date()
            except ValueError:
                continue

        logger.debug(f"Could not parse expiry date '{expiry_str}' with any known format")
        return None

    def _get_contract_month_from_date(self, expiry_date: date) -> str:
        """Get contract month from expiry date"""
        try:
            month_mapping = {1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN', 7: 'JUL', 8: 'AUG', 9: 'SEP', 10: 'OCT', 11: 'NOV', 12: 'DEC'}
            return month_mapping.get(expiry_date.month, 'UNK')
        except Exception:
            return 'UNK'

    def _safe_float(self, value, default: float = 0.0) -> float:
        """Safely convert value to float"""
        try:
            if pd.isna(value) or value in ["NA", "null"]:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    def _enrich_exchange_securities(self, exchange_code: str, exchange_securities: List[Dict[str, Any]], batch_size: int):
        """Enrich securities for a single exchange"""
        logger.info(f"Processing {len(exchange_securities)} securities for exchange: {exchange_code}")

        for i in range(0, len(exchange_securities), batch_size):
            batch = exchange_securities[i:i + batch_size]
            batch_symbols = [sec['symbol'] for sec in batch]

            # Build ISIN to security mapping for this batch
            isin_to_security = {sec['isin']: sec for sec in batch}

            try:
                sector_results = self.fetch_sector_info(None, batch_symbols, exchange_code)

                # Match by ISIN and update securities in place
                for sector_result in sector_results.values():
                    api_isin = sector_result.get('isin', '').strip()

                    if not api_isin:
                        continue

                    security = isin_to_security.get(api_isin)
                    if security:
                        with self._lock:  # Thread-safe updates
                            security['sector'] = sector_result.get('sector')
                            security['industry'] = sector_result.get('industry')

                # Rate limiting between batches
                if i + batch_size < len(exchange_securities):
                    import time
                    time.sleep(0.2)  # Reduced sleep for parallel processing

            except Exception as e:
                logger.warning(f"Error enriching batch {i//batch_size + 1} for exchange {exchange_code}: {e}")
                continue

    def _fetch_sector_info_bulk(self, url: str, symbols: List[str], exchange_code: str = "NSE") -> Dict[str, Any]:
        """Send one POST request for multiple symbols (comma-separated)"""
        try:
            symbol_string = ",".join(symbols)
            payload = {"data": {"fields": ["Sector", "SubSector"], "params": [{"field": "Exch", "op": "", "val": exchange_code}, {"field": "Sym", "op": "", "val": symbol_string}]}}

            logger.info(f"[Sector Enrichment] Requesting sector info for {len(symbols)} symbols on {exchange_code}: {symbol_string}")

            response = requests.post(url, json=payload, timeout=60)
            response.raise_for_status()

            data = response.json()

            if data.get("code") != 0 or "data" not in data:
                raise ExternalAPIError("Dhan", f"Invalid response for sector info from {exchange_code}")

            results = {}
            for item in data["data"]:
                api_isin = item.get("Isin", "").strip()
                if api_isin:
                    results[api_isin] = {"sector": item.get("Sector", "").strip(), "industry": item.get("SubSector", "").strip(), "symbol": item.get("DispSym", "").strip(), "isin": api_isin}

            logger.info(f"[Sector Enrichment] Received sector info for {len(results)} securities from {exchange_code}")
            return results

        except Exception as e:
            logger.warning(f"[Sector Enrichment] Error in bulk sector info fetch for symbols {symbols} on {exchange_code}: {e}")
            return {}
