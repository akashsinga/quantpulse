# app/scripts/fetch_data.py

import argparse
import sys
import json
import asyncio
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any

from app.services.data_fetchers import create_ohlcv_fetcher
from utils.logger import get_logger

logger = get_logger(__name__)


def parse_args():
    """Parse command line arguments with enhanced options."""
    parser = argparse.ArgumentParser(
        description="Enhanced QuantPulse OHLCV Data Fetcher - Maximum Performance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Enhanced Examples:
        # Maximum performance historical data fetch
        python -m app.scripts.fetch_data historical --workers=24 --batch-size=200 --verbose

        # High-speed current data fetch
        python -m app.scripts.fetch_data today --workers=16 --batch-size=1000

        # Comprehensive update with full history
        python -m app.scripts.fetch_data update-all --full-history --workers=20 --verbose

        # Specific exchange with performance monitoring
        python -m app.scripts.fetch_data update-all --exchanges=NSE --output-file=results.json

        # Health check and performance stats
        python -m app.scripts.fetch_data status --show-health --show-performance

        Enhanced Features:
        ✓ Optimized bulk database operations
        ✓ Enhanced error handling and circuit breaker
        ✓ Adaptive rate limiting with performance monitoring  
        ✓ Memory management and resource cleanup
        ✓ Comprehensive validation and data quality checks
        ✓ Real-time performance metrics and health monitoring

        Processing: STOCKS and INDICES only (derivatives excluded for maximum speed)
                """,
    )

    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Historical data command
    historical_parser = subparsers.add_parser("historical", help="Fetch historical OHLCV data with enhanced performance", description="Enhanced historical data fetching with optimized bulk operations")
    historical_parser.add_argument("--security-ids", help="Comma-separated list of security UUIDs")
    historical_parser.add_argument("--exchanges", help="Comma-separated list of exchange codes (e.g., NSE,BSE)")
    historical_parser.add_argument("--segments", help="Comma-separated list of segment types (e.g., EQUITY)")
    historical_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    historical_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    historical_parser.add_argument("--workers", type=int, default=16, help="Number of worker threads (default: 16, max: 32)")
    historical_parser.add_argument("--batch-size", type=int, default=200, help="Batch size for processing (default: 200)")
    historical_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    historical_parser.add_argument("--output-file", help="Output file for results (JSON)")
    historical_parser.add_argument("--full-history", action="store_true", help="Use full historical range from settings")
    historical_parser.add_argument("--show-performance", action="store_true", help="Show detailed performance metrics")

    # Today's data command
    today_parser = subparsers.add_parser("today", help="Fetch current day OHLCV data with high-speed processing", description="Enhanced current day data fetching with optimized API calls")
    today_parser.add_argument("--security-ids", help="Comma-separated list of security UUIDs")
    today_parser.add_argument("--exchanges", help="Comma-separated list of exchange codes")
    today_parser.add_argument("--segments", help="Comma-separated list of segment types")
    today_parser.add_argument("--eod", action="store_true", help="Run in end-of-day mode")
    today_parser.add_argument("--workers", type=int, default=12, help="Number of worker threads (default: 12)")
    today_parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for processing (default: 1000)")
    today_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    today_parser.add_argument("--output-file", help="Output file for results (JSON)")
    today_parser.add_argument("--show-performance", action="store_true", help="Show detailed performance metrics")

    # Update all command
    update_parser = subparsers.add_parser("update-all", help="Comprehensive data update with maximum performance", description="Enhanced comprehensive update with intelligent resource management")
    update_parser.add_argument("--skip-today", action="store_true", help="Skip today's data")
    update_parser.add_argument("--workers", type=int, default=20, help="Number of worker threads (default: 20)")
    update_parser.add_argument("--batch-size", type=int, default=200, help="Batch size for processing (default: 200)")
    update_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    update_parser.add_argument("--output-file", help="Output file for results (JSON)")
    update_parser.add_argument("--notification-email", help="Email for completion notification (future feature)")
    update_parser.add_argument("--show-performance", action="store_true", help="Show detailed performance metrics")

    # Status and maintenance commands
    status_parser = subparsers.add_parser("status", help="Check system status and performance metrics", description="Enhanced system status monitoring and performance reporting")
    status_parser.add_argument("--show-health", action="store_true", help="Show system health status")
    status_parser.add_argument("--show-performance", action="store_true", help="Show detailed performance metrics")
    status_parser.add_argument("--output-file", help="Output file for results (JSON)")

    # Data validation command
    validate_parser = subparsers.add_parser("validate", help="Validate data completeness and quality", description="Enhanced data validation with gap detection and quality checks")
    validate_parser.add_argument("--security-ids", required=True, help="Comma-separated list of security UUIDs to validate")
    validate_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    validate_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    validate_parser.add_argument("--output-file", help="Output file for results (JSON)")
    validate_parser.add_argument("--show-gaps", action="store_true", help="Show detailed gap information")

    # Reset metrics command
    reset_parser = subparsers.add_parser("reset-metrics", help="Reset all performance metrics", description="Reset all performance counters and metrics")
    reset_parser.add_argument("--confirm", action="store_true", help="Confirm metrics reset")

    args = parser.parse_args()

    # Ensure a command was specified
    if not args.command:
        parser.print_help()
        sys.exit(1)

    return args


def split_comma_separated(value: Optional[str]) -> Optional[List[str]]:
    """Split comma-separated string into list with enhanced validation."""
    if not value:
        return None

    result = []
    for item in value.split(","):
        item = item.strip()
        if item:  # Only add non-empty items
            result.append(item)

    return result if result else None


def save_output(result: Dict[str, Any], output_file: Optional[str]) -> None:
    """Save operation result to output file with enhanced JSON handling."""
    if not output_file:
        return

    try:
        # Custom JSON serializer for datetime objects
        def json_serializer(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        with open(output_file, "w") as f:
            json.dump(result, f, indent=2, default=json_serializer)

        logger.info(f"Results saved to {output_file}")

    except Exception as e:
        logger.error(f"Error saving results to {output_file}: {str(e)}")


def print_enhanced_summary(result: Dict[str, Any], command: str) -> None:
    """Print enhanced operation summary with better formatting."""
    print("\n" + "=" * 80)
    print(f"✨ ENHANCED QUANTPULSE OHLCV FETCHER - {command.upper()} OPERATION SUMMARY")
    print("=" * 80)
    print("📊 Processing: STOCKS & INDICES only (derivatives excluded for maximum speed)")
    print(f"⚡ Mode: Enhanced Performance with optimized DB operations and resource management")

    print(f"\n✅ Status: {result.get('status', 'Unknown')}")
    print(f"🔄 Operation ID: {result.get('operation_id', 'Unknown')}")
    print(f"⏱️  Duration: {result.get('duration_seconds', 0):.2f} seconds")

    if command == "historical":
        stats = result.get("stats", {})
        print(f"\n📈 Securities processed: {stats.get('securities_processed', 0)}")
        print(f"✅ Securities successful: {stats.get('securities_success', 0)}")
        print(f"❌ Securities with errors: {stats.get('securities_error', 0)}")
        print(f"📊 Total records: {stats.get('total_records', 0):,}")

        # Calculate processing rate
        perf = result.get("performance", {})
        print(f"🚀 Processing rate: {perf.get('securities_per_second', 0):.1f} securities/second")
        print(f"💾 Data throughput: {perf.get('records_per_second', 0):.1f} records/second")

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
        print(f"📜 Full history mode: {result.get('full_history', False)}")

        summary = result.get("summary", {})
        print(f"\n📊 Overall summary:")
        print(f"  └── Total operations: {summary.get('total_operations', 0)}")
        print(f"  └── Successful operations: {summary.get('successful_operations', 0)}")
        print(f"  └── Total records processed: {summary.get('total_records', 0):,}")
        print(f"  └── Total securities: {summary.get('total_securities', 0)}")

        if "historical" in result and result["historical"]:
            hist_stats = result["historical"].get("stats", {})
            print(f"\n📈 Historical data:")
            print(f"  └── Status: {result['historical'].get('status', 'unknown')}")
            print(f"  └── Securities processed: {hist_stats.get('securities_processed', 0)}")
            print(f"  └── Total records: {hist_stats.get('total_records', 0):,}")

        if "current" in result and result["current"]:
            curr_stats = result["current"].get("stats", {})
            print(f"\n📊 Today's data:")
            print(f"  └── Status: {result['current'].get('status', 'unknown')}")
            print(f"  └── Securities processed: {curr_stats.get('securities_processed', 0)}")
            print(f"  └── Total records stored: {curr_stats.get('total_records_stored', 0):,}")

    elif command == "status":
        health = result.get("health", {})
        if health:
            print(f"\n🏥 System Health: {health.get('status', 'unknown')}")

            components = health.get("components", {})
            for name, data in components.items():
                print(f"  └── {name}: {data.get('status', 'unknown')}")

        perf = result.get("performance", {})
        if perf:
            fetcher_stats = perf.get("fetcher", {})
            api_stats = perf.get("api_client", {})
            repo_stats = perf.get("repository", {})

            print(f"\n⚡ Performance Metrics:")
            print(f"  └── Operations: {fetcher_stats.get('operations_total', 0)} total, {fetcher_stats.get('success_rate_pct', 0):.1f}% success rate")
            print(f"  └── API calls: {api_stats.get('requests_total', 0)} total, {api_stats.get('success_rate_pct', 0):.1f}% success rate")
            print(f"  └── Database: {repo_stats.get('total_operations', 0)} operations, {repo_stats.get('total_records_processed', 0):,} records")
            print(f"  └── Cache hit rate: {fetcher_stats.get('cache_hit_rate_pct', 0):.1f}%")
            print(f"  └── Avg response time: {api_stats.get('avg_response_time_ms', 0):.2f}ms")

    elif command == "validate":
        validations = result.get("validations", {})
        if validations:
            print(f"\n🔍 Data Validation Results:")
            print(f"  └── Securities validated: {len(validations)}")

            complete = sum(1 for v in validations.values() if v.get("status") == "complete")
            with_gaps = sum(1 for v in validations.values() if v.get("status") == "gaps_found")
            errors = sum(1 for v in validations.values() if v.get("status") == "error")

            print(f"  └── Complete data: {complete} securities")
            print(f"  └── Data with gaps: {with_gaps} securities")
            print(f"  └── Validation errors: {errors} securities")

            total_gaps = sum(v.get("gap_count", 0) for v in validations.values())
            print(f"  └── Total gaps found: {total_gaps}")

            avg_coverage = sum(v.get("coverage_pct", 0) for v in validations.values()) / len(validations) if validations else 0
            print(f"  └── Average data coverage: {avg_coverage:.1f}%")

    # Performance insights
    print("\n" + "=" * 80)


async def execute_status_command(args):
    """Execute status command with enhanced health and performance checks."""
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    result = {"timestamp": datetime.now().isoformat(), "command": "status"}

    if args.show_health:
        result["health"] = fetcher.get_health_status()

    if args.show_performance:
        result["performance"] = fetcher.get_performance_stats()

    if not args.show_health and not args.show_performance:
        # If no specific flags, show both
        result["health"] = fetcher.get_health_status()
        result["performance"] = fetcher.get_performance_stats()

    # Save output if requested
    save_output(result, args.output_file)

    # Print summary
    print_enhanced_summary(result, "status")

    # Close fetcher
    await fetcher.close()

    return 0 if result.get("health", {}).get("status") == "healthy" else 1


async def execute_validate_command(args):
    """Execute data validation command."""
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Get arguments
    security_ids = split_comma_separated(args.security_ids)
    if not security_ids:
        logger.error("No security IDs provided for validation")
        return 1

    # Parse dates
    try:
        if args.start_date:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        else:
            # Default to 30 days back
            start_date = datetime.now().date() - timedelta(days=30)

        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        else:
            # Default to yesterday
            end_date = datetime.now().date() - timedelta(days=1)
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return 1

    logger.info(f"Validating data for {len(security_ids)} securities from {start_date} to {end_date}")

    # Perform validation
    result = {"timestamp": datetime.now().isoformat(), "command": "validate", "date_range": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()}, "validations": {}}

    for security_id in security_ids:
        try:
            validation = fetcher.repository.validate_data_continuity(security_id, start_date, end_date)

            # Simplify gap information unless --show-gaps is specified
            if not args.show_gaps and "gaps" in validation:
                validation["gap_count"] = len(validation["gaps"])
                if not validation["gaps"]:
                    validation["gaps"] = []
                elif len(validation["gaps"]) > 3:
                    validation["gaps"] = validation["gaps"][:3]
                    validation["gaps_truncated"] = True

            result["validations"][security_id] = validation

        except Exception as e:
            logger.error(f"Error validating security {security_id}: {e}")
            result["validations"][security_id] = {"security_id": security_id, "status": "error", "error": str(e)}

    # Save output if requested
    save_output(result, args.output_file)

    # Print summary
    print_enhanced_summary(result, "validate")

    # Close fetcher
    await fetcher.close()

    # Success if at least one validation succeeded
    return 0 if any(v.get("status") != "error" for v in result["validations"].values()) else 1


async def execute_reset_metrics_command(args):
    """Execute reset metrics command."""
    if not args.confirm:
        logger.error("Please use --confirm to reset metrics")
        print("\n⚠️  WARNING: This will reset all performance metrics. Use --confirm to proceed.")
        return 1

    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Reset metrics
    fetcher.reset_performance_stats()

    print("\n" + "=" * 80)
    print("✅ All performance metrics have been reset successfully")
    print("=" * 80)

    # Close fetcher
    await fetcher.close()

    return 0


def execute_historical_command(args):
    """Execute enhanced historical data fetch command."""
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Get arguments with enhanced validation
    security_ids = split_comma_separated(args.security_ids)
    exchanges = split_comma_separated(args.exchanges)
    segments = split_comma_separated(args.segments)
    workers = args.workers
    batch_size = args.batch_size
    verbose = args.verbose
    output_file = args.output_file
    full_history = args.full_history
    show_performance = args.show_performance

    # Cap workers for system stability
    if workers > 32:
        logger.warning(f"Workers capped at 32 for system stability (requested: {workers})")
        workers = 32

    # Cap batch size for memory efficiency
    if batch_size > 500:
        logger.warning(f"Batch size capped at 500 for memory efficiency (requested: {batch_size})")
        batch_size = 500

    # Handle full-history flag
    if full_history:
        start_date = None  # Will use default from settings
        end_date = None  # Will use current date
        logger.info("Using full history mode - processing all available data")
    else:
        start_date = args.start_date
        end_date = args.end_date

    logger.info(f"Starting enhanced historical fetch: {workers} workers, batch size {batch_size}")

    # Execute operation
    result = fetcher.fetch_historical_data(security_ids=security_ids, exchanges=exchanges, segments=segments, start_date=start_date, end_date=end_date, workers=workers, batch_size=batch_size, verbose=verbose)

    # Add performance stats if requested
    if show_performance:
        result["performance_details"] = fetcher.get_performance_stats()

    # Save output if requested
    save_output(result, output_file)

    # Print summary
    print_enhanced_summary(result, "historical")

    # Return success if no errors
    return 0 if result.get("status") == "completed" else 1


def execute_today_command(args):
    """Execute enhanced today's data fetch command."""
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Get arguments with enhanced validation
    security_ids = split_comma_separated(args.security_ids)
    exchanges = split_comma_separated(args.exchanges)
    segments = split_comma_separated(args.segments)
    is_eod = args.eod
    workers = args.workers
    batch_size = args.batch_size
    verbose = args.verbose
    output_file = args.output_file
    show_performance = args.show_performance

    # Cap workers for system stability
    if workers > 25:
        logger.warning(f"Workers capped at 25 for current data stability (requested: {workers})")
        workers = 25

    # Cap batch size for API limits
    if batch_size > 2000:
        logger.warning(f"Batch size capped at 2000 for API limits (requested: {batch_size})")
        batch_size = 2000

    logger.info(f"Starting enhanced current data fetch: {workers} workers, batch size {batch_size}, mode: {'EOD' if is_eod else 'Regular'}")

    # Execute operation
    result = fetcher.fetch_current_day_data(security_ids=security_ids, exchanges=exchanges, segments=segments, is_eod=is_eod, workers=workers, batch_size=batch_size, verbose=verbose)

    # Add performance stats if requested
    if show_performance:
        result["performance_details"] = fetcher.get_performance_stats()

    # Save output if requested
    save_output(result, output_file)

    # Print summary
    print_enhanced_summary(result, "today")

    # Return success if no errors
    return 0 if result.get("status") == "completed" else 1


def execute_update_all_command(args):
    """Execute enhanced update all command."""
    # Create fetcher
    fetcher = create_ohlcv_fetcher()

    # Get arguments with enhanced validation
    security_ids = split_comma_separated(args.security_ids)
    exchanges = split_comma_separated(args.exchanges)
    segments = split_comma_separated(args.segments)
    full_history = args.full_history
    days_back = args.days_back
    skip_today = args.skip_today
    workers = args.workers
    batch_size = args.batch_size
    verbose = args.verbose
    output_file = args.output_file
    notification_email = args.notification_email
    show_performance = args.show_performance

    # Cap workers for system stability
    if workers > 32:
        logger.warning(f"Workers capped at 32 for system stability (requested: {workers})")
        workers = 32

    # Cap batch size for memory efficiency
    if batch_size > 300:
        logger.warning(f"Batch size capped at 300 for memory efficiency (requested: {batch_size})")
        batch_size = 300

    mode_desc = "full history" if full_history else f"{days_back} days back"
    logger.info(f"Starting enhanced comprehensive update: {mode_desc}, include today: {not skip_today}, workers: {workers}")

    # Execute operation
    result = fetcher.update_all_data(security_ids=security_ids, exchanges=exchanges, segments=segments, days_back=days_back, include_today=not skip_today, workers=workers, batch_size=batch_size, verbose=verbose, full_history=full_history)

    # Add performance stats if requested
    if show_performance:
        result["performance_details"] = fetcher.get_performance_stats()

    # Add notification info if requested
    if notification_email:
        result["notification"] = {"email": notification_email, "sent": False, "message": "Email notification feature not implemented yet"}  # Placeholder for future email notification feature

    # Save output if requested
    save_output(result, output_file)

    # Print summary
    print_enhanced_summary(result, "update-all")

    # Return success if no errors
    return 0 if result.get("status") == "completed" else 1


async def main():
    """Enhanced main entry point with async support."""
    # Parse arguments
    args = parse_args()

    try:
        # Log startup with enhanced information
        logger.info(f"🚀 Starting Enhanced QuantPulse OHLCV Data Fetcher - Command: {args.command}")
        logger.info("⚡ Mode: MAXIMUM PERFORMANCE with optimized bulk operations")
        logger.info("📊 Focus: STOCKS & INDICES only (derivatives excluded for maximum speed)")

        # Execute appropriate command
        if args.command == "historical":
            return execute_historical_command(args)
        elif args.command == "today":
            return execute_today_command(args)
        elif args.command == "update-all":
            return execute_update_all_command(args)
        elif args.command == "status":
            return await execute_status_command(args)
        elif args.command == "validate":
            return await execute_validate_command(args)
        elif args.command == "reset-metrics":
            return await execute_reset_metrics_command(args)
        else:
            logger.error(f"Unknown command: {args.command}")
            return 1

    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
