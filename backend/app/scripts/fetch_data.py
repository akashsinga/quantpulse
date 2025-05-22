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
    parser = argparse.ArgumentParser(
        description="QuantPulse High-Performance OHLCV Data Fetcher - Stocks & Indices Only",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch historical data with high performance settings
  python -m app.scripts.fetch_data historical --workers=16 --batch-size=100

  # Fetch today's data for all securities
  python -m app.scripts.fetch_data today --workers=12 --batch-size=500

  # Full update with maximum performance
  python -m app.scripts.fetch_data update-all --full-history --workers=16

  # Update specific exchange with verbose output
  python -m app.scripts.fetch_data update-all --exchanges=NSE --verbose

Note: This fetcher processes STOCKS and INDICES only. Derivatives are excluded 
for faster processing and focused analysis on underlying securities.
        """,
    )

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Historical data command
    historical_parser = subparsers.add_parser("historical", help="Fetch historical OHLCV data for stocks and indices", description="Fetch historical OHLCV data with optimized performance settings")
    historical_parser.add_argument("--security-ids", help="Comma-separated list of security UUIDs")
    historical_parser.add_argument("--exchanges", help="Comma-separated list of exchange codes (e.g., NSE,BSE)")
    historical_parser.add_argument("--segments", help="Comma-separated list of segment types (e.g., EQUITY)")
    historical_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    historical_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    historical_parser.add_argument("--workers", type=int, default=24, help="Number of worker threads (default: 24, maximum performance)")
    historical_parser.add_argument("--batch-size", type=int, default=200, help="Batch size for processing (default: 200)")
    historical_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    historical_parser.add_argument("--output-file", help="Output file for results (JSON)")
    historical_parser.add_argument("--full-history", action="store_true", help="Use full historical range from settings")

    # Today's data command
    today_parser = subparsers.add_parser("today", help="Fetch today's OHLCV data for stocks and indices", description="Fetch current day data with high-speed batch processing")
    today_parser.add_argument("--security-ids", help="Comma-separated list of security UUIDs")
    today_parser.add_argument("--exchanges", help="Comma-separated list of exchange codes")
    today_parser.add_argument("--segments", help="Comma-separated list of segment types")
    today_parser.add_argument("--eod", action="store_true", help="Run in end-of-day mode")
    today_parser.add_argument("--workers", type=int, default=20, help="Number of worker threads (default: 20)")
    today_parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for processing (default: 1000)")
    today_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    today_parser.add_argument("--output-file", help="Output file for results (JSON)")

    # Update all command
    update_parser = subparsers.add_parser("update-all", help="High-performance update of both historical and today's data", description="Comprehensive data update with maximum performance settings")
    update_parser.add_argument("--full-history", action="store_true", help="Fetch full history for stocks instead of just recent data")
    update_parser.add_argument("--security-ids", help="Comma-separated list of security UUIDs")
    update_parser.add_argument("--exchanges", help="Comma-separated list of exchange codes")
    update_parser.add_argument("--segments", help="Comma-separated list of segment types")
    update_parser.add_argument("--days-back", type=int, default=7, help="Number of days back to check for gaps (default: 7)")
    update_parser.add_argument("--skip-today", action="store_true", help="Skip today's data")
    update_parser.add_argument("--workers", type=int, default=24, help="Number of worker threads (default: 24)")
    update_parser.add_argument("--batch-size", type=int, default=200, help="Batch size for processing (default: 200)")
    update_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    update_parser.add_argument("--output-file", help="Output file for results (JSON)")

    args = parser.parse_args()

    # Ensure a command was specified
    if not args.command:
        parser.print_help()
        sys.exit(1)

    return args


def split_comma_separated(value: Optional[str]) -> Optional[List[str]]:
    """Split comma-separated string into list.

    Args:
        value: Comma-separated string

    Returns:
        List of strings or None if input is None
    """
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def save_output(result: Dict[str, Any], output_file: Optional[str]) -> None:
    """Save operation result to output file if specified.

    Args:
        result: Operation result
        output_file: Output file path
    """
    if output_file:
        try:
            # Convert datetime objects to strings for JSON serialization
            def json_serial(obj):
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")

            with open(output_file, "w") as f:
                json.dump(result, f, indent=2, default=json_serial)
            logger.info(f"Results saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving results to {output_file}: {str(e)}")


def print_summary(result: Dict[str, Any], command: str) -> None:
    """Print operation summary to console.

    Args:
        result: Operation result
        command: Command name
    """
    print("\n" + "=" * 80)
    print(f"HIGH-PERFORMANCE OHLCV FETCHER - {command.upper()} OPERATION SUMMARY")
    print("=" * 80)
    print("📊 Processing: STOCKS & INDICES only (derivatives excluded for speed)")
    print("⚡ Mode: High-Performance with optimized concurrency")

    print(f"\n✅ Status: {result.get('status', 'Unknown')}")
    print(f"⏱️  Duration: {result.get('duration_seconds', 0):.2f} seconds")

    if command == "historical":
        stats = result.get("stats", {})
        print(f"\n📈 Securities processed: {stats.get('securities_processed', 0)}")
        print(f"✅ Securities successful: {stats.get('securities_success', 0)}")
        print(f"❌ Securities with errors: {stats.get('securities_error', 0)}")
        print(f"📊 Total records: {stats.get('total_records', 0):,}")

        # Calculate processing rate
        duration = result.get("duration_seconds", 1)
        rate = stats.get("securities_processed", 0) / duration if duration > 0 else 0
        print(f"🚀 Processing rate: {rate:.1f} securities/second")

    elif command == "today":
        stats = result.get("stats", {})
        print(f"\n📈 Securities processed: {stats.get('securities_processed', 0)}")
        print(f"📊 Securities with data: {stats.get('securities_with_data', 0)}")
        print(f"🔍 Securities without data: {stats.get('securities_without_data', 0)}")
        print(f"❌ Securities with errors: {stats.get('securities_error', 0)}")
        print(f"💾 Total records stored: {stats.get('total_records_stored', 0):,}")

    elif command == "update-all":
        print(f"\n🔄 Days back: {result.get('days_back', 0)}")
        print(f"📅 Include today: {not result.get('skip_today', False)}")

        if "historical" in result and result["historical"]:
            hist_stats = result["historical"].get("stats", {})
            print(f"\n📈 Historical data:")
            print(f"  └── Securities processed: {hist_stats.get('securities_processed', 0)}")
            print(f"  └── Securities successful: {hist_stats.get('securities_success', 0)}")
            print(f"  └── Total records: {hist_stats.get('total_records', 0):,}")

        if "current" in result and result["current"]:
            curr_stats = result["current"].get("stats", {})
            print(f"\n📊 Today's data:")
            print(f"  └── Securities processed: {curr_stats.get('securities_processed', 0)}")
            print(f"  └── Securities with data: {curr_stats.get('securities_with_data', 0)}")
            print(f"  └── Total records stored: {curr_stats.get('total_records_stored', 0):,}")

    # Performance insights
    total_records = 0
    if command == "historical":
        total_records = result.get("stats", {}).get("total_records", 0)
    elif command == "today":
        total_records = result.get("stats", {}).get("total_records_stored", 0)
    elif command == "update-all":
        hist_records = result.get("historical", {}).get("stats", {}).get("total_records", 0)
        curr_records = result.get("current", {}).get("stats", {}).get("total_records_stored", 0)
        total_records = hist_records + curr_records

    duration = result.get("duration_seconds", 1)
    if total_records > 0 and duration > 0:
        records_per_sec = total_records / duration
        print(f"\n🚀 Performance: {records_per_sec:.0f} records/second")

    print("\n" + "=" * 80)


def execute_historical_command(args):
    """Execute historical data fetch command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Get arguments with safer access
    security_ids = split_comma_separated(getattr(args, "security_ids", None))
    exchanges = split_comma_separated(getattr(args, "exchanges", None))
    segments = split_comma_separated(getattr(args, "segments", None))
    workers = getattr(args, "workers", 16)
    batch_size = getattr(args, "batch_size", 100)
    verbose = getattr(args, "verbose", False)
    output_file = getattr(args, "output_file", None)
    full_history = getattr(args, "full_history", False)

    # Validate performance settings
    if workers > 32:
        logger.warning(f"Workers set to {workers}, capping at 32 for system stability")
        workers = 32
    if batch_size > 300:
        logger.warning(f"Batch size set to {batch_size}, capping at 300 for memory efficiency")
        batch_size = 300

    # Handle full-history flag
    if full_history:
        start_date = None  # Will use default from settings
        end_date = None  # Will use current date
        logger.info("Using full history mode - processing all available data")
    else:
        start_date = getattr(args, "start_date", None)
        end_date = getattr(args, "end_date", None)

    logger.info(f"Starting MAXIMUM PERFORMANCE historical fetch: {workers} workers, batch size {batch_size}")

    # Execute operation
    result = fetcher.fetch_historical_data(security_ids=security_ids, exchanges=exchanges, segments=segments, start_date=start_date, end_date=end_date, workers=workers, batch_size=batch_size, verbose=verbose)

    # Save output if requested
    save_output(result, output_file)

    # Print summary
    print_summary(result, "historical")

    # Return success if no errors
    return 0 if result.get("status") == "completed" else 1


def execute_today_command(args):
    """Execute today's data fetch command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Get arguments with safer access
    security_ids = split_comma_separated(getattr(args, "security_ids", None))
    exchanges = split_comma_separated(getattr(args, "exchanges", None))
    segments = split_comma_separated(getattr(args, "segments", None))
    is_eod = getattr(args, "eod", False)
    workers = getattr(args, "workers", 12)
    batch_size = getattr(args, "batch_size", 500)
    verbose = getattr(args, "verbose", False)
    output_file = getattr(args, "output_file", None)

    # Validate performance settings
    if workers > 25:
        logger.warning(f"Workers set to {workers}, capping at 25 for current data stability")
        workers = 25
    if batch_size > 2000:
        logger.warning(f"Batch size set to {batch_size}, capping at 2000 for API limits")
        batch_size = 2000

    logger.info(f"Starting ULTRA-FAST current data fetch: {workers} workers, batch size {batch_size}")

    # Execute operation
    result = fetcher.fetch_current_day_data(security_ids=security_ids, exchanges=exchanges, segments=segments, is_eod=is_eod, workers=workers, batch_size=batch_size, verbose=verbose)

    # Save output if requested
    save_output(result, output_file)

    # Print summary
    print_summary(result, "today")

    # Return success if no errors
    return 0 if result.get("status") == "completed" else 1


def execute_update_all_command(args):
    """Execute update all command.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Get arguments with safer access
    security_ids = split_comma_separated(getattr(args, "security_ids", None))
    exchanges = split_comma_separated(getattr(args, "exchanges", None))
    segments = split_comma_separated(getattr(args, "segments", None))
    full_history = getattr(args, "full_history", False)
    days_back = getattr(args, "days_back", 7)
    skip_today = getattr(args, "skip_today", False)
    workers = getattr(args, "workers", 16)
    batch_size = getattr(args, "batch_size", 100)
    verbose = getattr(args, "verbose", False)
    output_file = getattr(args, "output_file", None)

    # Validate performance settings
    if workers > 32:
        logger.warning(f"Workers set to {workers}, capping at 32 for stability")
        workers = 32
    if batch_size > 300:
        logger.warning(f"Batch size set to {batch_size}, capping at 300 for memory efficiency")
        batch_size = 300

    mode_desc = "full history" if full_history else f"{days_back} days back"
    logger.info(f"Starting MAXIMUM PERFORMANCE comprehensive update: {mode_desc}, {workers} workers")

    # Execute operation
    result = fetcher.update_all_data(security_ids=security_ids, exchanges=exchanges, segments=segments, days_back=days_back, include_today=not skip_today, workers=workers, batch_size=batch_size, verbose=verbose, full_history=full_history)

    # Save output if requested
    save_output(result, output_file)

    # Print summary
    print_summary(result, "update-all")

    # Return success if no errors
    return 0 if result.get("status") == "completed" else 1


def main():
    """Main entry point for CLI.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    # Parse arguments
    args = parse_args()

    try:
        # Log startup with performance info
        logger.info(f"🚀 Starting MAXIMUM PERFORMANCE OHLCV Data Fetcher - Command: {args.command}")
        logger.info("⚡ Mode: ULTRA-FAST processing with 50 concurrent requests")
        logger.info("📊 Focus: STOCKS & INDICES only (derivatives excluded for maximum speed)")

        # Execute appropriate command
        if args.command == "historical":
            return execute_historical_command(args)
        elif args.command == "today":
            return execute_today_command(args)
        elif args.command == "update-all":
            return execute_update_all_command(args)
        else:
            logger.error(f"Unknown command: {args.command}")
            return 1
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        print(f"\n❌ ERROR: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
