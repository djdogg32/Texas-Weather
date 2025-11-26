import schedule
import time
import logging
from datetime import datetime
import os
import sys

# Import your existing pipeline
# Make sure your weather pipeline file is in the same directory or adjust the import
# If your file is named 'weather_pipeline.py', this will work
try:
    from weather_pipeline import WeatherPipeline
except ImportError:
    print("⚠️  Could not import WeatherPipeline. Make sure weather_pipeline.py is in the same directory.")
    sys.exit(1)

"""
AUTOMATED WEATHER PIPELINE SCHEDULER
Runs your weather ETL pipeline on a schedule automatically
"""


# ============================================================================
# CONFIGURATION
# ============================================================================

class AutomationConfig:
    """Configuration for automation"""

    # Schedule settings
    RUN_INTERVAL_HOURS = 3  # Run every 3 hours
    RUN_AT_TIME = "06:00"  # Or run at specific time daily

    # Logging
    LOG_DIR = "logs"
    LOG_FILE = "pipeline_automation.log"

    # Error handling
    MAX_RETRIES = 3
    RETRY_DELAY_MINUTES = 5


# ============================================================================
# SETUP LOGGING
# ============================================================================

def setup_logging():
    """Setup logging configuration"""

    # Create logs directory
    if not os.path.exists(AutomationConfig.LOG_DIR):
        os.makedirs(AutomationConfig.LOG_DIR)

    log_file = os.path.join(AutomationConfig.LOG_DIR, AutomationConfig.LOG_FILE)

    # Configure logging with UTF-8 encoding for emojis
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)  # Use stdout with UTF-8
        ]
    )

    # Set console encoding to UTF-8 for Windows
    if sys.platform == 'win32':
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except:
            pass  # If it fails, continue anyway

    return logging.getLogger(__name__)


logger = setup_logging()


# ============================================================================
# PIPELINE EXECUTION WITH ERROR HANDLING
# ============================================================================

def run_pipeline_with_retry():
    """Run the pipeline with retry logic"""

    for attempt in range(1, AutomationConfig.MAX_RETRIES + 1):
        try:
            logger.info("=" * 70)
            logger.info(f"🚀 Starting pipeline execution (Attempt {attempt}/{AutomationConfig.MAX_RETRIES})")
            logger.info("=" * 70)

            # Run the pipeline
            pipeline = WeatherPipeline()
            result = pipeline.run()

            if result:
                logger.info("✅ Pipeline completed successfully!")
                logger.info(f"   - Execution time: {result['execution_time']:.2f} seconds")
                logger.info(f"   - Current weather records: {len(result['data']['current'])}")
                logger.info(f"   - Forecast records: {len(result['data']['forecast'])}")
                return True
            else:
                logger.error("❌ Pipeline returned no result")

        except Exception as e:
            logger.error(f"❌ Pipeline failed on attempt {attempt}: {str(e)}")

            if attempt < AutomationConfig.MAX_RETRIES:
                wait_seconds = AutomationConfig.RETRY_DELAY_MINUTES * 60
                logger.info(f"⏳ Retrying in {AutomationConfig.RETRY_DELAY_MINUTES} minutes...")
                time.sleep(wait_seconds)
            else:
                logger.error(f"❌ Pipeline failed after {AutomationConfig.MAX_RETRIES} attempts")
                return False

    return False


# ============================================================================
# SCHEDULED JOB
# ============================================================================

def scheduled_job():
    """The job that runs on schedule"""
    logger.info("\n" + "🔔 " * 35)
    logger.info("SCHEDULED JOB TRIGGERED")
    logger.info("🔔 " * 35)

    success = run_pipeline_with_retry()

    if success:
        logger.info("✅ Scheduled job completed successfully\n")
    else:
        logger.error("❌ Scheduled job failed\n")


# ============================================================================
# SCHEDULE CONFIGURATION
# ============================================================================

def setup_schedule():
    """Setup the schedule"""

    logger.info("⚙️  Setting up automation schedule...")

    # OPTION 1: Run every X hours
    schedule.every(AutomationConfig.RUN_INTERVAL_HOURS).hours.do(scheduled_job)
    logger.info(f"   ✓ Scheduled to run every {AutomationConfig.RUN_INTERVAL_HOURS} hours")

    # OPTION 2: Run at specific time daily (comment out if using OPTION 1)
    # schedule.every().day.at(AutomationConfig.RUN_AT_TIME).do(scheduled_job)
    # logger.info(f"   ✓ Scheduled to run daily at {AutomationConfig.RUN_AT_TIME}")

    # OPTION 3: Multiple times per day (comment out if using OPTION 1)
    # schedule.every().day.at("06:00").do(scheduled_job)
    # schedule.every().day.at("12:00").do(scheduled_job)
    # schedule.every().day.at("18:00").do(scheduled_job)
    # logger.info("   ✓ Scheduled to run at 6 AM, 12 PM, and 6 PM daily")

    logger.info("✅ Schedule configured successfully\n")


# ============================================================================
# MAIN AUTOMATION LOOP
# ============================================================================

def main():
    """Main automation loop"""

    logger.info("\n" + "=" * 70)
    logger.info("🤖 WEATHER PIPELINE AUTOMATION STARTED")
    logger.info("=" * 70)
    logger.info(f"📅 Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📁 Logs directory: {AutomationConfig.LOG_DIR}")
    logger.info(f"🔄 Run interval: Every {AutomationConfig.RUN_INTERVAL_HOURS} hours")
    logger.info("=" * 70 + "\n")

    # Setup schedule
    setup_schedule()

    # Run once immediately on startup
    logger.info("🚀 Running initial pipeline execution...")
    scheduled_job()

    # Keep running scheduled tasks
    logger.info("\n⏰ Automation loop started. Press Ctrl+C to stop.\n")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

    except KeyboardInterrupt:
        logger.info("\n" + "=" * 70)
        logger.info("⏹️  Automation stopped by user")
        logger.info("=" * 70)


# ============================================================================
# ALTERNATIVE: Run Once and Exit
# ============================================================================

def run_once():
    """Run the pipeline once and exit (useful for cron jobs)"""
    logger.info("Running pipeline once...")
    success = run_pipeline_with_retry()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    # Default: Run continuously with schedule
    main()

    # Alternative: Run once and exit (uncomment for cron jobs)
    # run_once()