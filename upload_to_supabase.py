import os
import requests
from datetime import datetime

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BUCKET = os.environ.get("SUPABASE_BUCKET", "downloads")


def main():
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"downloads-{today}.csv"

    with open(filename, "rb") as f:
        content = f.read()

    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{filename}?upsert=true"
    headers = {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "text/csv"
    }

    response = requests.put(url, headers=headers, data=content)

    if response.ok:
        print(f"✅ Uploaded to Supabase: {filename}")
    else:
        print(f"❌ Upload failed: {response.status_code} — {response.text}")


if __name__ == "__main__":
    main()
