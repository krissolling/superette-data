"""
Superette Store Data Scraper
Fetches all products and collections from superettestore.com
Stores data in SQLite with full change history tracking
"""

import requests
import json
import sqlite3
import os
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = "https://superettestore.com"
DATA_DIR = Path("data")
DB_FILE = DATA_DIR / "superette.db"


def get_db_connection():
    """Get a connection to the SQLite database."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """Initialize the database schema."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Products table - current state
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            handle TEXT NOT NULL,
            title TEXT NOT NULL,
            body_html TEXT,
            vendor TEXT,
            product_type TEXT,
            tags TEXT,
            price TEXT,
            compare_at_price TEXT,
            available INTEGER,
            sku TEXT,
            image_url TEXT,
            shopify_created_at TEXT,
            shopify_updated_at TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            is_active INTEGER DEFAULT 1
        )
    """)

    # Products history - track all changes
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            changed_at TEXT NOT NULL
        )
    """)

    # Collections table - current state
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY,
            handle TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            products_count INTEGER,
            image_url TEXT,
            shopify_updated_at TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            is_active INTEGER DEFAULT 1
        )
    """)

    # Scrape log - track each scrape run
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at TEXT NOT NULL,
            products_total INTEGER,
            products_added INTEGER,
            products_removed INTEGER,
            products_updated INTEGER,
            collections_total INTEGER,
            collections_added INTEGER,
            collections_removed INTEGER,
            summary TEXT
        )
    """)

    # Create indexes for common queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_active ON products(is_active)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_vendor ON products(vendor)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_type ON products(product_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_product ON products_history(product_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_date ON products_history(changed_at)")

    conn.commit()
    conn.close()


def fetch_all_products():
    """Fetch all products from the store, handling pagination."""
    all_products = []
    page = 1

    while True:
        url = f"{BASE_URL}/products.json?limit=250&page={page}"
        print(f"Fetching products page {page}...")

        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        products = data.get("products", [])
        if not products:
            break

        all_products.extend(products)
        page += 1

    print(f"Fetched {len(all_products)} products total")
    return all_products


def fetch_all_collections():
    """Fetch all collections from the store, handling pagination."""
    all_collections = []
    page = 1

    while True:
        url = f"{BASE_URL}/collections.json?page={page}"
        print(f"Fetching collections page {page}...")

        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        collections = data.get("collections", [])
        if not collections:
            break

        all_collections.extend(collections)
        page += 1

    print(f"Fetched {len(all_collections)} collections total")
    return all_collections


def extract_product_data(product):
    """Extract relevant fields from a product API response."""
    variant = product["variants"][0] if product.get("variants") else {}
    image = product["images"][0] if product.get("images") else {}

    return {
        "id": product["id"],
        "handle": product["handle"],
        "title": product["title"],
        "body_html": product.get("body_html", ""),
        "vendor": product.get("vendor", ""),
        "product_type": product.get("product_type", ""),
        "tags": json.dumps(product.get("tags", [])),
        "price": variant.get("price"),
        "compare_at_price": variant.get("compare_at_price"),
        "available": 1 if variant.get("available") else 0,
        "sku": variant.get("sku"),
        "image_url": image.get("src"),
        "shopify_created_at": product.get("created_at"),
        "shopify_updated_at": product.get("updated_at"),
    }


def extract_collection_data(collection):
    """Extract relevant fields from a collection API response."""
    image = collection.get("image") or {}

    return {
        "id": collection["id"],
        "handle": collection["handle"],
        "title": collection["title"],
        "description": collection.get("description", ""),
        "products_count": collection.get("products_count", 0),
        "image_url": image.get("src"),
        "shopify_updated_at": collection.get("updated_at"),
    }


def sync_products(products):
    """
    Sync products to database, tracking all changes.
    Returns dict with counts of added, removed, updated products.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    # Get existing active products
    cursor.execute("SELECT id FROM products WHERE is_active = 1")
    existing_ids = {row["id"] for row in cursor.fetchall()}

    new_ids = set()
    added = []
    updated = []

    for product in products:
        data = extract_product_data(product)
        product_id = data["id"]
        new_ids.add(product_id)

        # Check if product exists
        cursor.execute("SELECT * FROM products WHERE id = ?", (product_id,))
        existing = cursor.fetchone()

        if existing is None:
            # New product
            cursor.execute("""
                INSERT INTO products (
                    id, handle, title, body_html, vendor, product_type, tags,
                    price, compare_at_price, available, sku, image_url,
                    shopify_created_at, shopify_updated_at, first_seen_at, last_seen_at, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (
                data["id"], data["handle"], data["title"], data["body_html"],
                data["vendor"], data["product_type"], data["tags"], data["price"],
                data["compare_at_price"], data["available"], data["sku"], data["image_url"],
                data["shopify_created_at"], data["shopify_updated_at"], now, now
            ))
            added.append({"id": product_id, "title": data["title"], "vendor": data["vendor"], "price": data["price"]})

        else:
            # Existing product - check for changes
            changes = []
            fields_to_check = ["title", "price", "available", "vendor", "product_type", "compare_at_price", "sku"]

            for field in fields_to_check:
                old_val = str(existing[field]) if existing[field] is not None else None
                new_val = str(data[field]) if data[field] is not None else None

                if old_val != new_val:
                    changes.append((field, old_val, new_val))
                    cursor.execute("""
                        INSERT INTO products_history (product_id, field_name, old_value, new_value, changed_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (product_id, field, old_val, new_val, now))

            # Update the product
            cursor.execute("""
                UPDATE products SET
                    handle = ?, title = ?, body_html = ?, vendor = ?, product_type = ?, tags = ?,
                    price = ?, compare_at_price = ?, available = ?, sku = ?, image_url = ?,
                    shopify_created_at = ?, shopify_updated_at = ?, last_seen_at = ?, is_active = 1
                WHERE id = ?
            """, (
                data["handle"], data["title"], data["body_html"], data["vendor"],
                data["product_type"], data["tags"], data["price"], data["compare_at_price"],
                data["available"], data["sku"], data["image_url"], data["shopify_created_at"],
                data["shopify_updated_at"], now, product_id
            ))

            if changes:
                updated.append({
                    "id": product_id,
                    "title": data["title"],
                    "changes": [{"field": c[0], "old": c[1], "new": c[2]} for c in changes]
                })

    # Mark removed products as inactive
    removed_ids = existing_ids - new_ids
    removed = []

    for product_id in removed_ids:
        cursor.execute("SELECT title, vendor FROM products WHERE id = ?", (product_id,))
        row = cursor.fetchone()
        removed.append({"id": product_id, "title": row["title"], "vendor": row["vendor"]})

        cursor.execute("UPDATE products SET is_active = 0, last_seen_at = ? WHERE id = ?", (now, product_id))
        cursor.execute("""
            INSERT INTO products_history (product_id, field_name, old_value, new_value, changed_at)
            VALUES (?, 'is_active', '1', '0', ?)
        """, (product_id, now))

    conn.commit()
    conn.close()

    return {"added": added, "removed": removed, "updated": updated}


def sync_collections(collections):
    """Sync collections to database, tracking changes."""
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    cursor.execute("SELECT id FROM collections WHERE is_active = 1")
    existing_ids = {row["id"] for row in cursor.fetchall()}

    new_ids = set()
    added = []
    removed = []
    product_count_changes = []

    for collection in collections:
        data = extract_collection_data(collection)
        collection_id = data["id"]
        new_ids.add(collection_id)

        cursor.execute("SELECT * FROM collections WHERE id = ?", (collection_id,))
        existing = cursor.fetchone()

        if existing is None:
            cursor.execute("""
                INSERT INTO collections (
                    id, handle, title, description, products_count, image_url,
                    shopify_updated_at, first_seen_at, last_seen_at, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (
                data["id"], data["handle"], data["title"], data["description"],
                data["products_count"], data["image_url"], data["shopify_updated_at"], now, now
            ))
            added.append({"id": collection_id, "title": data["title"]})
        else:
            # Check for product count changes
            old_count = existing["products_count"]
            new_count = data["products_count"]
            if old_count != new_count:
                product_count_changes.append({
                    "id": collection_id,
                    "title": data["title"],
                    "old_count": old_count,
                    "new_count": new_count,
                    "change": new_count - old_count
                })

            cursor.execute("""
                UPDATE collections SET
                    handle = ?, title = ?, description = ?, products_count = ?, image_url = ?,
                    shopify_updated_at = ?, last_seen_at = ?, is_active = 1
                WHERE id = ?
            """, (
                data["handle"], data["title"], data["description"], data["products_count"],
                data["image_url"], data["shopify_updated_at"], now, collection_id
            ))

    # Mark removed collections
    for collection_id in (existing_ids - new_ids):
        cursor.execute("SELECT title FROM collections WHERE id = ?", (collection_id,))
        row = cursor.fetchone()
        removed.append({"id": collection_id, "title": row["title"]})
        cursor.execute("UPDATE collections SET is_active = 0, last_seen_at = ? WHERE id = ?", (now, collection_id))

    conn.commit()
    conn.close()

    return {"added": added, "removed": removed, "product_count_changes": product_count_changes}


def log_scrape(products_total, product_changes, collections_total, collection_changes, summary):
    """Log the scrape run to the database."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO scrape_log (
            scraped_at, products_total, products_added, products_removed, products_updated,
            collections_total, collections_added, collections_removed, summary
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now(timezone.utc).isoformat(),
        products_total,
        len(product_changes["added"]),
        len(product_changes["removed"]),
        len(product_changes["updated"]),
        collections_total,
        len(collection_changes["added"]),
        len(collection_changes["removed"]),
        summary
    ))

    conn.commit()
    conn.close()


def generate_summary(product_changes, collection_changes, products_count, collections_count):
    """Generate a human-readable summary of the scrape."""
    lines = [
        "=" * 60,
        f"SUPERETTE DATA SCRAPE - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 60,
        "",
        f"📦 Total Products: {products_count}",
        f"📁 Total Collections: {collections_count}",
        "",
        "📊 CHANGES:",
        f"   ➕ New products: {len(product_changes['added'])}",
        f"   ➖ Removed products: {len(product_changes['removed'])}",
        f"   🔄 Updated products: {len(product_changes['updated'])}",
    ]

    if product_changes["added"]:
        lines.append("")
        lines.append("   NEW PRODUCTS:")
        for p in product_changes["added"][:10]:
            lines.append(f"      • {p['title']} (£{p['price']}) - {p['vendor']}")
        if len(product_changes["added"]) > 10:
            lines.append(f"      ... and {len(product_changes['added']) - 10} more")

    if product_changes["removed"]:
        lines.append("")
        lines.append("   REMOVED PRODUCTS:")
        for p in product_changes["removed"][:10]:
            lines.append(f"      • {p['title']} - {p['vendor']}")
        if len(product_changes["removed"]) > 10:
            lines.append(f"      ... and {len(product_changes['removed']) - 10} more")

    if product_changes["updated"]:
        lines.append("")
        lines.append("   UPDATED PRODUCTS:")
        for p in product_changes["updated"][:10]:
            changes_str = ", ".join([f"{c['field']}: {c['old']} → {c['new']}" for c in p["changes"][:2]])
            lines.append(f"      • {p['title']}: {changes_str}")
        if len(product_changes["updated"]) > 10:
            lines.append(f"      ... and {len(product_changes['updated']) - 10} more")

    if collection_changes["product_count_changes"]:
        lines.append("")
        lines.append("   COLLECTION CHANGES:")
        for c in collection_changes["product_count_changes"]:
            change_str = f"+{c['change']}" if c['change'] > 0 else str(c['change'])
            lines.append(f"      • {c['title']}: {c['old_count']} → {c['new_count']} ({change_str})")

    lines.append("")
    lines.append("=" * 60)

    return "\n".join(lines)


def main():
    """Main scraper function."""
    print("Starting Superette data scrape...")
    print()

    # Initialize database
    init_database()

    # Fetch new data
    products = fetch_all_products()
    collections = fetch_all_collections()

    # Sync to database
    print("\nSyncing products to database...")
    product_changes = sync_products(products)

    print("Syncing collections to database...")
    collection_changes = sync_collections(collections)

    # Generate summary
    summary = generate_summary(product_changes, collection_changes, len(products), len(collections))
    print(summary)

    # Log the scrape
    log_scrape(len(products), product_changes, len(collections), collection_changes, summary)

    # Save summary to file for GitHub Actions
    summary_file = DATA_DIR / "latest_summary.txt"
    with open(summary_file, "w") as f:
        f.write(summary)

    # Set output for GitHub Actions
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"products_count={len(products)}\n")
            f.write(f"collections_count={len(collections)}\n")
            f.write(f"added_count={len(product_changes['added'])}\n")
            f.write(f"removed_count={len(product_changes['removed'])}\n")
            f.write(f"updated_count={len(product_changes['updated'])}\n")

    print("\n✅ Scrape complete!")
    return product_changes, collection_changes


if __name__ == "__main__":
    main()
