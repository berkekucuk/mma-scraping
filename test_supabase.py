from ufc_scraper.services.supabase_manager import SupabaseClient
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_connection():
    """Test Supabase connection and basic queries."""

    print("\n" + "=" * 60)
    print("🔌 TESTING SUPABASE CONNECTION")
    print("=" * 60)

    # Test 1: Get client
    try:
        client = SupabaseClient.get_client()
        print("✅ Step 1: Client initialized")
    except Exception as e:
        print(f"❌ Step 1 FAILED: {e}")
        print("\n💡 Tip: Check your .env file and Supabase credentials")
        return

    # Test 2: Connection test
    print("\n🔍 Testing database connection...")
    if SupabaseClient.test_connection():
        print("✅ Step 2: Connection successful")
    else:
        print("❌ Step 2 FAILED: Cannot connect to database")
        return

    # Test 3: Count records
    try:
        print("\n📊 Fetching database statistics...")

        fighters = client.table("fighters").select("*", count="exact").execute()
        events = client.table("events").select("*", count="exact").execute()
        fights = client.table("fights").select("*", count="exact").execute()
        participants = client.table("participants").select("*", count="exact").execute()

        print("\n" + "=" * 60)
        print("📈 DATABASE STATISTICS")
        print("=" * 60)
        print(f"👤 Fighters:      {fighters.count:>6,}")
        print(f"🎟️  Events:        {events.count:>6,}")
        print(f"🥊 Fights:        {fights.count:>6,}")
        print(f"👥 Participants:  {participants.count:>6,}")

    except Exception as e:
        print(f"❌ Step 3 FAILED: {e}")
        return

    # Test 4: Sample data
    try:
        print("\n🔍 Fetching sample fighter data...")
        sample = client.table("fighters").select("fighter_id, name, nickname, country_code").limit(3).execute()

        if sample.data:
            print("\n" + "=" * 60)
            print("📋 SAMPLE FIGHTER DATA")
            print("=" * 60)
            for fighter in sample.data:
                nickname = f"'{fighter.get('nickname')}'" if fighter.get('nickname') else "N/A"
                print(f"🥋 {fighter.get('name')} ({nickname}) - {fighter.get('country_code')}")

    except Exception as e:
        print(f"⚠️  Warning: Could not fetch sample data: {e}")

    print("\n" + "=" * 60)
    print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print("\n💡 Next step: Run 'scrapy crawl ufc_events' or 'scrapy crawl fighter'\n")

if __name__ == "__main__":
    test_connection()
