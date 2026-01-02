import subprocess
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):

    if 'task' in event:
        task_type = event.get('task')
        logger.info(f"[SCHEDULED] Task triggered: {task_type}")

        try:
            if task_type == 'live':
                subprocess.run(["scrapy", "crawl", "smart", "-a", "mode=live", "--loglevel", "INFO"], check=True)
            elif task_type == 'upcoming':
                subprocess.run(["scrapy", "crawl", "smart", "-a", "mode=upcoming", "--loglevel", "INFO"], check=True)
            else:
                return {"statusCode": 400, "body": "Undefined task"}

            return {"statusCode": 200, "body": f"Scheduled task '{task_type}' completed"}

        except subprocess.CalledProcessError as e:
            logger.error(f"Scheduled spider failed: {str(e)}")
            raise e


    elif 'body' in event:
        try:
            body = json.loads(event['body'])

            table = body.get('table')
            op_type = body.get('type')
            record = body.get('record', {})

            if table == 'fighters' and op_type == 'INSERT':

                fighter_id = record.get('fighter_id')
                profile_url = record.get('profile_url')

                if not profile_url:
                     logger.warning(f"[WEBHOOK] Fighter {fighter_id} has no profile URL. Skipping.")
                     return {"statusCode": 200, "body": "No URL, skipped"}

                logger.info(f"[WEBHOOK] Rescuing fighter: {fighter_id} from {profile_url}")

                subprocess.run([
                    "scrapy", "crawl", "fighter",
                    "-a", f"profile_url={profile_url}",
                    "-a", f"fighter_id={fighter_id}",
                    "--loglevel", "INFO"
                ], check=True)

                return {"statusCode": 200, "body": f"Rescue scrape finished for {fighter_id}"}

            else:
                logger.info(f"[WEBHOOK] Ignored event: Table={table}, Type={op_type}")
                return {"statusCode": 200, "body": "Ignored"}

        except json.JSONDecodeError:
            logger.error("Failed to decode Webhook JSON body")
            return {"statusCode": 400, "body": "Invalid JSON"}
        except subprocess.CalledProcessError as e:
            logger.error(f"Rescue spider failed: {str(e)}")
            raise e
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise e

    else:
        logger.warning("Unknown event structure.")
        return {"statusCode": 400, "body": "Unknown event"}
