import subprocess
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Main function that AWS Lambda will call.
    The event parameter will contain { "task": "live" } or { "task": "upcoming" } information.
    """
    task_type = event.get('task', 'live')

    logger.info(f"Lambda triggered. Task type: {task_type}")

    try:
        if task_type == 'live':
            logger.info("Starting smart spider (live mode)...")
            subprocess.run(["scrapy", "crawl", "smart", "-a", "mode=live", "--loglevel", "INFO"], check=True)

        elif task_type == 'upcoming':
            logger.info("Starting smart spider (upcoming mode)...")
            subprocess.run(["scrapy", "crawl", "smart", "-a", "mode=upcoming", "--loglevel", "INFO"], check=True)

        else:
            logger.warning(f"Undefined task type: {task_type}")
            return {"statusCode": 400, "body": "Undefined task"}

    except subprocess.CalledProcessError as e:
        logger.error(f"Process failed with error: {str(e)}")
        raise e

    logger.info("Process completed successfully.")
    return {"statusCode": 200, "body": "Success"}
