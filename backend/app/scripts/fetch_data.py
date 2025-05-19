# app/cli/commands/fetch_data.py

import argparse
import sys
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any

from app.services.data_fetchers import create_ohlcv_fetcher
from utils.logger import get_logger

logger = get_logger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="QuantPulse OHLCV Data Fetcher")

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Historical data command
    historical_parser = subparsers.add_parser("historical", help="Fetch historical OHLCV data")
    historical_parser.add_argument("--security-ids", help="Comma-separated list of security UUIDs")
    historical_parser.add_argument("--segments", help="Comma-separated list of segment types")
    historical_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    historical_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    historical_parser.add_argument("--workers", type=int, default=8, help="Number of worker threads")
    historical_parser.add_argument("--batch-size", type=int, default=50, help="Batch size for processing")
    historical_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    historical_parser.add_argument("--output-file", help="Output file for results (JSON)")
    historical_parser.add_argument("--full-history", action="store_true", help="Use full historical range from settings")

    # Today's data command
    today_parser = subparsers.add_parser("today", help="Fetch today's OHLCV data")
    today_parser.add_argument("--security-ids", help="Comma-separated list of security UUIDs")
    today_parser.add_argument("--exchanges", help="Comma-separated list of exchange codes")
    today_parser.add_argument("--segments", help="Comma-separated list of segment types")
    today_parser.add_argument("--eod", action="store_true", help="Run in end-of-day mode")
    today_parser.add_argument("--workers", type=int, default=4, help="Number of worker threads")
    today_parser.add_argument("--batch-size", type=int, default=200, help="Batch size for processing")
    today_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    today_parser.add_argument("--output-file", help="Output file for results (JSON)")

    # Update all command
    update_parser = subparsers.add_parser("update-all", help="Update both historical and today's data")
    update_parser.add_argument("--security-ids", help="Comma-separated list of security UUIDs")
    update_parser.add_argument("--exchanges", help="Comma-separated list of exchange codes")
    update_parser.add_argument("--segments", help="Comma-separated list of segment types")
    update_parser.add_argument("--days-back", type=int, default=7, help="Number of days back to check for gaps")
    update_parser.add_argument("--skip-today", action="store_true", help="Skip today's data")
    update_parser.add_argument("--workers", type=int, default=8, help="Number of worker threads")
    update_parser.add_argument("--batch-size", type=int, default=50, help="Batch size for processing")
    update_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    update_parser.add_argument("--output-file", help="Output file for results (JSON)")

    return parser.parse_args()


def split_comma_separated(value: Optional[str]) -> Optional[List[str]]:
    """Split comma-separated string into list."""
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def save_output(result: Dict[str, Any], output_file: Optional[str]) -> None:
    """Save operation result to output file if specified."""
    if output_file:
        try:
            with open(output_file, "w") as f:
                json.dump(result, f, indent=2, default=str)
            logger.info(f"Results saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving results to {output_file}: {str(e)}")


def print_summary(result: Dict[str, Any], command: str) -> None:
    """Print operation summary to console."""
    print("\n" + "=" * 80)
    print(f"OHLCV Data Fetcher - {command.upper()} OPERATION SUMMARY")
    print("=" * 80)

    print(f"Status: {result.get('status', 'Unknown')}")
    print(f"Duration: {result.get('duration_seconds', 0):.2f} seconds")

    if command == "historical":
        stats = result.get("stats", {})
        print(f"\nSecurities processed: {stats.get('securities_processed', 0)}")
        print(f"Securities successful: {stats.get('securities_success', 0)}")
        print(f"Securities with errors: {stats.get('securities_error', 0)}")
        print(f"Total records: {stats.get('total_records', 0)}")

    elif command == "today":
        stats = result.get("stats", {})
        print(f"\nSecurities processed: {stats.get('securities_processed', 0)}")
        print(f"Securities with data: {stats.get('securities_with_data', 0)}")
        print(f"Securities without data: {stats.get('securities_without_data', 0)}")
        print(f"Securities with errors: {stats.get('securities_error', 0)}")
        print(f"Total records stored: {stats.get('total_records_stored', 0)}")

    elif command == "update-all":
        print(f"\nDays back: {result.get('days_back', 0)}")
        print(f"Include today: {not result.get('skip_today', False)}")

        if "historical" in result and result["historical"]:
            hist_stats = result["historical"].get("stats", {})
            print(f"\nHistorical data:")
            print(f"  Securities processed: {hist_stats.get('securities_processed', 0)}")
            print(f"  Securities successful: {hist_stats.get('securities_success', 0)}")
            print(f"  Total records: {hist_stats.get('total_records', 0)}")

        if "current" in result and result["current"]:
            curr_stats = result["current"].get("stats", {})
            print(f"\nToday's data:")
            print(f"  Securities processed: {curr_stats.get('securities_processed', 0)}")
            print(f"  Securities with data: {curr_stats.get('securities_with_data', 0)}")
            print(f"  Total records stored: {curr_stats.get('total_records_stored', 0)}")

    print("\n" + "=" * 80)


def execute_historical_command(args):
    """Execute historical data fetch command."""
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Parse security_ids, exchanges, segments
    security_ids = split_comma_separated(args.security_ids)
    exchanges = split_comma_separated(args.exchanges)
    segments = split_comma_separated(args.segments)

    # Handle full-history flag
    if args.full_history:
        start_date = None  # Will use default from settings
        end_date = None  # Will use current date
    else:
        start_date = args.start_date
        end_date = args.end_date

    # Execute operation
    result = fetcher.fetch_historical_data(security_ids=security_ids, exchanges=exchanges, segments=segments, start_date=start_date, end_date=end_date, workers=args.workers, batch_size=args.batch_size, verbose=args.verbose)

    # Save output if requested
    save_output(result, args.output_file)

    # Print summary
    print_summary(result, "historical")

    # Return success if no errors
    return 0 if result.get("status") == "completed" else 1


def execute_today_command(args):
    """Execute today's data fetch command."""
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Parse security_ids, exchanges, segments
    security_ids = split_comma_separated(args.security_ids)
    exchanges = split_comma_separated(args.exchanges)
    segments = split_comma_separated(args.segments)

    # Execute operation
    result = fetcher.fetch_current_day_data(security_ids=security_ids, exchanges=exchanges, segments=segments, is_eod=args.eod, workers=args.workers, batch_size=args.batch_size, verbose=args.verbose)

    # Save output if requested
    save_output(result, args.output_file)

    # Print summary
    print_summary(result, "today")

    # Return success if no errors
    return 0 if result.get("status") == "completed" else 1


def execute_update_all_command(args):
    """Execute update all command."""
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Parse security_ids, exchanges, segments
    security_ids = split_comma_separated(args.security_ids)
    exchanges = split_comma_separated(args.exchanges)
    segments = split_comma_separated(args.segments)

    # Execute operation
    result = fetcher.update_all_data(security_ids=security_ids, exchanges=exchanges, segments=segments, days_back=args.days_back, include_today=not args.skip_today, workers=args.workers, batch_size=args.batch_size, verbose=args.verbose)

    # Save output if requested
    save_output(result, args.output_file)

    # Print summary
    print_summary(result, "update-all")

    # Return success if no errors
    return 0 if result.get("status") == "completed" else 1


def main():
    """Main entry point for CLI."""
    # Parse arguments
    args = parse_args()

    # Execute appropriate command
    if args.command == "historical":
        return execute_historical_command(args)
    elif args.command == "today":
        return execute_today_command(args)
    elif args.command == "update-all":
        return execute_update_all_command(args)
    else:
        logger.error("No command specified. Use --help for usage information.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
