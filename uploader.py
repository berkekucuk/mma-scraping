import os
from supabase import create_client, Client
from tqdm import tqdm
from dotenv import load_dotenv
load_dotenv()  # Bu satırı import'lardan sonra, config kısmından önce ekleyin
# -------------------------
# CONFIG
# -------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET_NAME = "fighter-images"

# Local directory for images
BASE_DIR = "data/fighter_images"

FAILED_LOG = "failed_uploads.txt"

# -------------------------
# INIT SUPABASE CLIENT
# -------------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def ensure_bucket():
    """Create bucket if it does not exist."""
    try:
        # Check if bucket exists by trying to get it
        supabase.storage.get_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' already exists.")
    except Exception:
        # Bucket doesn't exist, create it
        print(f"📦 Bucket '{BUCKET_NAME}' not found. Creating...")
        try:
            supabase.storage.create_bucket(BUCKET_NAME, options={"public": True})
            print("✅ Bucket created.")
        except Exception as e:
            print(f"❌ Failed to create bucket: {e}")
            raise


def upload_file(local_path, storage_path):
    """Upload file to Supabase storage."""
    try:
        with open(local_path, "rb") as f:
            supabase.storage.from_(BUCKET_NAME).upload(
                path=storage_path,
                file=f,
                file_options={"content-type": "image/jpeg"}
            )
        return True
    except Exception as e:
        print(f"❌ Upload failed for {storage_path}: {e}")
        return False


def main():
    ensure_bucket()

    # gather all image files
    all_files = os.listdir(BASE_DIR)
    image_files = [
        f for f in all_files
        if f.lower().endswith((".png", ".jpg", ".jpeg"))
    ]

    print(f"📸 Found {len(image_files)} images to upload in '{BASE_DIR}'")

    failed = []

    for filename in tqdm(image_files, desc="Uploading images"):
        local_path = os.path.join(BASE_DIR, filename)

        # directly use filename as storage key, e.g. '25031.png'
        storage_path = filename

        # upload
        ok = upload_file(local_path, storage_path)
        if not ok:
            failed.append(filename)

    # log failures
    if failed:
        with open(FAILED_LOG, "w") as f:
            for name in failed:
                f.write(name + "\n")
        print(f"⚠️ {len(failed)} upload failed. See {FAILED_LOG}")
    else:
        print("🎉 All images uploaded successfully!")


if __name__ == "__main__":
    main()
