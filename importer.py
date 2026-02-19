import os
import io
import csv
import gzip
import json
import requests
from datetime import datetime, timezone

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
AWIN_FEED_URL = os.environ["AWIN_FEED_URL"]

HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}

def supabase_upsert(table: str, rows: list, on_conflict: str = None):
    if not rows:
        return

    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {}
    if on_conflict:
        params["on_conflict"] = on_conflict

    r = requests.post(url, headers=HEADERS, params=params, data=json.dumps(rows))
    if r.status_code >= 300:
        raise RuntimeError(f"Supabase upsert failed {table}: {r.status_code} {r.text}")

def supabase_select_one(table: str, query: str):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{query}&limit=1"
    r = requests.get(url, headers=HEADERS)
    if r.status_code >= 300:
        raise RuntimeError(f"Supabase select failed {table}: {r.status_code} {r.text}")
    data = r.json()
    return data[0] if data else None

def main():
    print("Downloading Awin feed...")
    resp = requests.get(AWIN_FEED_URL, timeout=120)
    resp.raise_for_status()

    # Feed is gzip-compressed per your settings
    gz = gzip.GzipFile(fileobj=io.BytesIO(resp.content))
    text = gz.read().decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    now = datetime.now(timezone.utc).isoformat()

    new_queue_rows = []
    product_rows = []
    offer_rows = []
    raw_rows = []

    count = 0
    for row in reader:
        count += 1

        # We use EAN as the best universal matcher across retailers
        ean = (row.get("ean") or "").strip()
        if not ean:
            # Skip products without EAN — optional, but recommended
            continue

        product_name = (row.get("product_name") or "").strip()
        brand = (row.get("brand_name") or "").strip() or None

        # --- 1) Save raw snapshot row (debugging / traceability)
        raw_rows.append({
            "aw_product_id": (row.get("aw_product_id") or "").strip(),
            "ean": ean,
            "product_name": product_name,
            "raw_json": row
        })

        # --- 2) Upsert canonical product
        # Minimal fields for now; we can enrich later
        product_rows.append({
            "ean": ean,
            "brand": brand,
            "model": None,
            "model_year": None,
            "product_name": product_name,
            "updated_at": now
        })

        # --- 3) Upsert offer (Padel Market)
        offer_rows.append({
            "product_ean": ean,
            "retailer": (row.get("merchant_name") or "Padel Market").strip(),
            "network": "awin",
            "price": float(row.get("search_price") or row.get("store_price") or 0) or None,
            "currency": (row.get("currency") or "").strip() or None,
            "in_stock": str(row.get("in_stock") or "").strip().lower() in ["1", "true", "yes"],
            "affiliate_link": (row.get("aw_deep_link") or "").strip(),
            "last_seen": now
        })

        # --- 4) Queue NEW products for review
        # Check if it already exists in products table
        existing = supabase_select_one("products", f"ean=eq.{ean}")
        if not existing:
            new_queue_rows.append({
                "product_ean": ean,
                "reason": "new_product"
            })

        # Keep memory safe for big feeds (bulk upsert in chunks)
        if len(raw_rows) >= 500:
            supabase_upsert("product_raw_feed", raw_rows)
            raw_rows = []
        if len(product_rows) >= 500:
            supabase_upsert("products", product_rows, on_conflict="ean")
            product_rows = []
        if len(offer_rows) >= 500:
            supabase_upsert("offers", offer_rows)
            offer_rows = []
        if len(new_queue_rows) >= 100:
            supabase_upsert("review_queue", new_queue_rows)
            new_queue_rows = []

    # flush leftovers
    supabase_upsert("product_raw_feed", raw_rows)
    supabase_upsert("products", product_rows, on_conflict="ean")
    supabase_upsert("offers", offer_rows)
    supabase_upsert("review_queue", new_queue_rows)

    print(f"Done. Processed rows: {count}")

if __name__ == "__main__":
    main()
