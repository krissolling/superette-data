"""
Superette Store Data Scraper
Fetches all products and collections from superettestore.com
Tracks changes: new products, removed products, and updates
"""

import requests
import json
import os
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = "https://superettestore.com"
DATA_DIR = Path("data")
PRODUCTS_FILE = DATA_DIR / "products.json"
COLLECTIONS_FILE = DATA_DIR / "collections.json"
CHANGELOG_FILE = DATA_DIR / "changelog.json"


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


def load_existing_data(filepath):
    """Load existing data from a JSON file."""
    if filepath.exists():
        with open(filepath, "r") as f:
            return json.load(f)
    return None


def save_data(filepath, data):
    """Save data to a JSON file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


def detect_product_changes(old_products, new_products):
    """
    Compare old and new product lists to detect changes.
    Returns: dict with added, removed, and updated products
    """
    if old_products is None:
        # First run - all products are "new" but we don't log them as additions
        return {"added": [], "removed": [], "updated": [], "first_run": True}

    old_by_id = {p["id"]: p for p in old_products.get("products", [])}
    new_by_id = {p["id"]: p for p in new_products}

    old_ids = set(old_by_id.keys())
    new_ids = set(new_by_id.keys())

    # Find added products
    added_ids = new_ids - old_ids
    added = [
        {
            "id": pid,
            "title": new_by_id[pid]["title"],
            "handle": new_by_id[pid]["handle"],
            "vendor": new_by_id[pid]["vendor"],
            "product_type": new_by_id[pid]["product_type"],
            "price": new_by_id[pid]["variants"][0]["price"] if new_by_id[pid]["variants"] else None,
        }
        for pid in added_ids
    ]

    # Find removed products
    removed_ids = old_ids - new_ids
    removed = [
        {
            "id": pid,
            "title": old_by_id[pid]["title"],
            "handle": old_by_id[pid]["handle"],
            "vendor": old_by_id[pid]["vendor"],
            "product_type": old_by_id[pid]["product_type"],
        }
        for pid in removed_ids
    ]

    # Find updated products (based on updated_at timestamp)
    common_ids = old_ids & new_ids
    updated = []
    for pid in common_ids:
        old_updated = old_by_id[pid].get("updated_at", "")
        new_updated = new_by_id[pid].get("updated_at", "")

        if old_updated != new_updated:
            # Detect what changed
            changes = []
            old_p = old_by_id[pid]
            new_p = new_by_id[pid]

            if old_p.get("title") != new_p.get("title"):
                changes.append(f"title: '{old_p.get('title')}' -> '{new_p.get('title')}'")

            old_price = old_p["variants"][0]["price"] if old_p.get("variants") else None
            new_price = new_p["variants"][0]["price"] if new_p.get("variants") else None
            if old_price != new_price:
                changes.append(f"price: £{old_price} -> £{new_price}")

            old_available = old_p["variants"][0]["available"] if old_p.get("variants") else None
            new_available = new_p["variants"][0]["available"] if new_p.get("variants") else None
            if old_available != new_available:
                changes.append(f"availability: {old_available} -> {new_available}")

            updated.append({
                "id": pid,
                "title": new_p["title"],
                "handle": new_p["handle"],
                "changes": changes if changes else ["metadata updated"],
            })

    return {
        "added": added,
        "removed": removed,
        "updated": updated,
        "first_run": False,
    }


def detect_collection_changes(old_collections, new_collections):
    """Compare old and new collection lists to detect changes."""
    if old_collections is None:
        return {"added": [], "removed": [], "product_count_changes": [], "first_run": True}

    old_by_id = {c["id"]: c for c in old_collections.get("collections", [])}
    new_by_id = {c["id"]: c for c in new_collections}

    old_ids = set(old_by_id.keys())
    new_ids = set(new_by_id.keys())

    added = [{"id": cid, "title": new_by_id[cid]["title"]} for cid in (new_ids - old_ids)]
    removed = [{"id": cid, "title": old_by_id[cid]["title"]} for cid in (old_ids - new_ids)]

    # Check for product count changes
    product_count_changes = []
    for cid in (old_ids & new_ids):
        old_count = old_by_id[cid].get("products_count", 0)
        new_count = new_by_id[cid].get("products_count", 0)
        if old_count != new_count:
            product_count_changes.append({
                "id": cid,
                "title": new_by_id[cid]["title"],
                "old_count": old_count,
                "new_count": new_count,
                "change": new_count - old_count,
            })

    return {
        "added": added,
        "removed": removed,
        "product_count_changes": product_count_changes,
        "first_run": False,
    }


def update_changelog(product_changes, collection_changes):
    """Append changes to the changelog file."""
    changelog = load_existing_data(CHANGELOG_FILE) or {"entries": []}

    timestamp = datetime.now(timezone.utc).isoformat()

    entry = {
        "timestamp": timestamp,
        "products": {
            "added_count": len(product_changes["added"]),
            "removed_count": len(product_changes["removed"]),
            "updated_count": len(product_changes["updated"]),
            "added": product_changes["added"],
            "removed": product_changes["removed"],
            "updated": product_changes["updated"],
        },
        "collections": {
            "added_count": len(collection_changes["added"]),
            "removed_count": len(collection_changes["removed"]),
            "product_count_changes": collection_changes["product_count_changes"],
        },
    }

    # Only add entry if there are actual changes (or it's the first run)
    has_changes = (
        product_changes.get("first_run") or
        collection_changes.get("first_run") or
        product_changes["added"] or
        product_changes["removed"] or
        product_changes["updated"] or
        collection_changes["added"] or
        collection_changes["removed"] or
        collection_changes["product_count_changes"]
    )

    if has_changes:
        if product_changes.get("first_run"):
            entry["note"] = "Initial data fetch"
        changelog["entries"].append(entry)
        save_data(CHANGELOG_FILE, changelog)
        return entry

    return None


def generate_summary(product_changes, collection_changes, products, collections):
    """Generate a human-readable summary of the scrape."""
    lines = [
        "=" * 60,
        f"SUPERETTE DATA SCRAPE - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 60,
        "",
        f"📦 Total Products: {len(products)}",
        f"📁 Total Collections: {len(collections)}",
        "",
    ]

    if product_changes.get("first_run"):
        lines.append("🆕 First run - initial data captured")
    else:
        lines.append("📊 CHANGES DETECTED:")
        lines.append(f"   ➕ New products: {len(product_changes['added'])}")
        lines.append(f"   ➖ Removed products: {len(product_changes['removed'])}")
        lines.append(f"   🔄 Updated products: {len(product_changes['updated'])}")

        if product_changes["added"]:
            lines.append("")
            lines.append("   NEW PRODUCTS:")
            for p in product_changes["added"][:10]:  # Show first 10
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
                changes_str = ", ".join(p["changes"][:2])
                lines.append(f"      • {p['title']}: {changes_str}")
            if len(product_changes["updated"]) > 10:
                lines.append(f"      ... and {len(product_changes['updated']) - 10} more")

        if collection_changes["product_count_changes"]:
            lines.append("")
            lines.append("   COLLECTION CHANGES:")
            for c in collection_changes["product_count_changes"]:
                change_str = f"+{c['change']}" if c['change'] > 0 else str(c['change'])
                lines.append(f"      • {c['title']}: {c['old_count']} -> {c['new_count']} ({change_str})")

    lines.append("")
    lines.append("=" * 60)

    return "\n".join(lines)


def main():
    """Main scraper function."""
    print("Starting Superette data scrape...")
    print()

    # Load existing data
    old_products = load_existing_data(PRODUCTS_FILE)
    old_collections = load_existing_data(COLLECTIONS_FILE)

    # Fetch new data
    new_products = fetch_all_products()
    new_collections = fetch_all_collections()

    # Detect changes
    product_changes = detect_product_changes(old_products, new_products)
    collection_changes = detect_collection_changes(old_collections, new_collections)

    # Save new data with metadata
    products_data = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "count": len(new_products),
        "products": new_products,
    }

    collections_data = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "count": len(new_collections),
        "collections": new_collections,
    }

    save_data(PRODUCTS_FILE, products_data)
    save_data(COLLECTIONS_FILE, collections_data)

    # Update changelog
    changelog_entry = update_changelog(product_changes, collection_changes)

    # Generate and print summary
    summary = generate_summary(product_changes, collection_changes, new_products, new_collections)
    print(summary)

    # Save summary to file for GitHub Actions
    summary_file = DATA_DIR / "latest_summary.txt"
    with open(summary_file, "w") as f:
        f.write(summary)

    # Set output for GitHub Actions
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"products_count={len(new_products)}\n")
            f.write(f"collections_count={len(new_collections)}\n")
            f.write(f"added_count={len(product_changes['added'])}\n")
            f.write(f"removed_count={len(product_changes['removed'])}\n")
            f.write(f"updated_count={len(product_changes['updated'])}\n")

    print("\n✅ Scrape complete!")
    return product_changes, collection_changes


if __name__ == "__main__":
    main()
