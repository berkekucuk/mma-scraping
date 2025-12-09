import sys
import subprocess
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Main function that AWS Lambda will call.
    The event parameter will contain { "task": "live" } or { "task": "upcoming" } information.
    """
    task_type = event.get('task', 'live') # Default to live

    logger.info(f"Lambda triggered. Task type: {task_type}")

    try:
        if task_type == 'live':
            # Weekend task "Live Match Check"
            # Runs the run_live_scraper.py file
            logger.info("Starting live scraper script...")
            subprocess.run([sys.executable, "run_live_scraper.py"], check=True)

        elif task_type == 'upcoming':
            # Weekday task "Scrape Upcoming Matches"
            # Directly runs the scrapy command
            logger.info("Starting smart spider (upcoming)...")
            # Note: We print logs to console instead of writing to file (so CloudWatch can capture them)
            subprocess.run(["scrapy", "crawl", "smart", "--loglevel", "INFO"], check=True)

        else:
            logger.warning(f"Undefined task type: {task_type}")
            return {"statusCode": 400, "body": "Undefined task"}

    except subprocess.CalledProcessError as e:
        logger.error(f"Process failed with error: {str(e)}")
        # Raise exception to mark Lambda as failed
        raise e

    logger.info("Process completed successfully.")
    return {"statusCode": 200, "body": "Success"}
