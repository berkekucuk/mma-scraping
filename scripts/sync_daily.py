import os
import asyncio
from datetime import datetime, timedelta, timezone
from supabase import acreate_client, AsyncClient

from dotenv import load_dotenv
load_dotenv()

PROD_URL = os.getenv("SUPABASE_PROD_URL")
PROD_KEY = os.getenv("SUPABASE_PROD_KEY")
BACKUP_URL = os.getenv("SUPABASE_DEV_URL")
BACKUP_KEY = os.getenv("SUPABASE_DEV_KEY")

SYNC_TABLES = [
    "events",
    "fighters",
    "fights",
    "rankings",
    "participants"
]

async def sync_table(prod_client: AsyncClient, backup_client: AsyncClient, table_name: str):
    print(f"--- Syncing table: {table_name} ---")

    yesterday = datetime.now(timezone.utc) - timedelta(hours=25)

    try:
        response = await prod_client.table(table_name)\
            .select("*")\
            .gt("updated_at", yesterday.isoformat())\
            .execute()

        data = response.data

        if not data:
            print(f"No changes found for {table_name}.")
            return

        print(f"Found {len(data)} records to upsert in {table_name}...")

        await backup_client.table(table_name).upsert(data).execute()
        print(f"✅ Successfully synced {len(data)} records to {table_name}.")

    except Exception as e:
        print(f"❌ ERROR syncing {table_name}: {e}")
        raise e

async def main():
    if not all([PROD_URL, PROD_KEY, BACKUP_URL, BACKUP_KEY]):
        print("Error: Missing environment variables.")
        exit(1)

    prod_client = await acreate_client(PROD_URL, PROD_KEY)
    backup_client = await acreate_client(BACKUP_URL, BACKUP_KEY)

    print(f"Starting Daily Sync at {datetime.now(timezone.utc)}")

    try:
        for table in SYNC_TABLES:
            await sync_table(prod_client, backup_client, table)

        print("\n🎉 Daily Sync Completed Successfully!")

    except Exception as e:
        print(f"\n💀 Sync Failed due to error: {e}")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
