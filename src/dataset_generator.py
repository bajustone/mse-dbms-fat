"""
Synthetic e-commerce dataset generator for ULK MSE Final Project.
Generates 90 days of data with Rwandan localization.

Usage: python src/dataset_generator.py
"""

import json
import os
import random
import secrets
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from faker import Faker

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NUM_USERS = 2000
NUM_CATEGORIES = 15
NUM_PRODUCTS = 800
NUM_SESSIONS = 20000
SESSION_FILE_COUNT = 5  # split sessions across this many files

DATE_START = datetime(2025, 12, 1, tzinfo=timezone.utc)
DATE_END = datetime(2026, 3, 1, tzinfo=timezone.utc)
DATE_RANGE_SECONDS = int((DATE_END - DATE_START).total_seconds())

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

fake = Faker()
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------------------
# Rwandan localization data
# ---------------------------------------------------------------------------
RWANDAN_FIRST_NAMES = [
    "Mutesi", "Uwimana", "Mugisha", "Habimana", "Uwase", "Ingabire",
    "Niyonzima", "Mukamana", "Iradukunda", "Ishimwe", "Manzi", "Ndayisaba",
    "Uwitonze", "Nshimiyimana", "Umutoni", "Kamanzi", "Tuyisenge", "Mukiza",
    "Nisingizwe", "Nsabimana", "Uwera", "Hirwa", "Dusabe", "Muhire",
    "Umulisa", "Cyiza", "Rugamba", "Kayitesi", "Ndikumana", "Twagirumukiza",
    "Izere", "Gasana", "Bimenyimana", "Iyamuremye", "Nizeyimana",
    "Mukagatare", "Shyaka", "Bayisenge", "Nyiraneza", "Rurangwa",
]

RWANDAN_LAST_NAMES = [
    "Mugabo", "Habiyambere", "Ndahayo", "Uwimana", "Nkurunziza",
    "Hakizimana", "Bizimungu", "Munyakazi", "Gasana", "Rwigamba",
    "Ntawuyirushintege", "Mbonyumutwa", "Karangwa", "Nsengimana",
    "Rutayisire", "Mukeshimana", "Habyarimana", "Kalisa", "Ruzindana",
    "Nyirahabimana", "Twahirwa", "Sibomana", "Nzabonimpa", "Gatera",
    "Umubyeyi", "Niyigena", "Kabera", "Murenzi", "Rudasingwa", "Kayiranga",
]

RWANDAN_CITIES_PROVINCES = [
    ("Kigali", "Kigali City"),
    ("Huye", "Southern"),
    ("Rubavu", "Western"),
    ("Musanze", "Northern"),
    ("Muhanga", "Southern"),
    ("Rwamagana", "Eastern"),
    ("Nyagatare", "Eastern"),
    ("Rusizi", "Western"),
    ("Karongi", "Western"),
    ("Nyanza", "Southern"),
    ("Byumba", "Northern"),
    ("Kibungo", "Eastern"),
    ("Kibuye", "Western"),
    ("Gitarama", "Southern"),
    ("Butare", "Southern"),
]

PROVINCES = list({p for _, p in RWANDAN_CITIES_PROVINCES})

DEVICE_COMBOS = [
    {"type": "mobile", "os": "Android", "browser": "Chrome"},
    {"type": "mobile", "os": "iOS", "browser": "Safari"},
    {"type": "mobile", "os": "Android", "browser": "Samsung Internet"},
    {"type": "desktop", "os": "Windows", "browser": "Chrome"},
    {"type": "desktop", "os": "Windows", "browser": "Edge"},
    {"type": "desktop", "os": "macOS", "browser": "Safari"},
    {"type": "desktop", "os": "macOS", "browser": "Chrome"},
    {"type": "desktop", "os": "Linux", "browser": "Firefox"},
    {"type": "tablet", "os": "iOS", "browser": "Safari"},
    {"type": "tablet", "os": "Android", "browser": "Chrome"},
]

REFERRERS = ["search_engine", "direct", "social_media", "email", "affiliate"]
PAYMENT_METHODS = ["mtn_momo", "airtel_money", "bank_transfer", "visa", "cash_on_delivery"]
STATUS_CHOICES = ["completed"] * 5 + ["shipped"] * 3 + ["processing"] * 1 + ["refunded"] * 1
PAGE_TYPES = ["home", "category", "product", "search", "cart", "checkout"]

# Rwandan market product names by category
RWANDAN_PRODUCTS = {
    "Electronics": [
        "Samsung Galaxy A14", "Tecno Spark 10", "Itel A60", "JBL Flip Speaker",
        'Hisense 32" TV', "Oraimo Earbuds", "Infinix Hot 30",
        "Nokia Power Bank 10000mAh", "LG Microwave 20L", "Hikvision CCTV Camera",
        "TP-Link WiFi Router", "Tecno Camon 20", 'Samsung 43" Smart TV',
        "Anker USB-C Charger", "Redmi 12C", "Universal Remote Control",
        "Deepcool Laptop Cooler", "Sony Headphones WH-1000", "HP 15 Laptop",
        "Lenovo IdeaPad 3",
    ],
    "Clothing": [
        "Kitenge Dress", "Mushanana", "Ankara Shirt", "Umwitero Sash",
        "Men's Polo Shirt", "Denim Jeans", "Leather Sandals", "Igitenge Skirt",
        "Cotton T-Shirt", "Khaki Trousers", "Women's Blouse", "Sports Jersey",
        "Ankara Headwrap", "School Uniform Set", "Work Boots", "Rain Jacket",
        "Fleece Hoodie", "Formal Suit", "Baby Romper", "Wax Print Fabric 6yd",
    ],
    "Home & Garden": [
        "Plastic Water Tank 1000L", "Solar Panel 100W",
        "Charcoal Stove (Imbabura)", "Bed Mattress Queen", "Mosquito Net",
        "Curtain Set", "Dinner Plate Set", "Cooking Pot 10L", "Broom & Dustpan",
        "Plastic Chair", "Wall Clock", "LED Bulb Pack", "Garden Hoe (Isuka)",
        "Watering Can", "Washing Basin", "Thermos Flask 2L", "Bedsheet Set",
        "Bucket 20L", "Ironing Board", "Dish Rack",
    ],
    "Sports": [
        "Football (Soccer Ball)", "Running Shoes", "Gym Dumbbell Set",
        "Cycling Jersey", "Basketball", "Yoga Mat", "Resistance Bands",
        "Sports Bag", "Boxing Gloves", "Volleyball Net", "Tennis Racket",
        "Swimming Goggles", "Skipping Rope", "Fitness Tracker Band",
        "Water Bottle 1L",
    ],
    "Books": [
        "Kinyarwanda Dictionary", "History of Rwanda", "French Textbook",
        "English Grammar Guide", "Mathematics Secondary", "Bible (Kinyarwanda)",
        "KCSE Past Papers", "Children's Storybook", "Accounting Principles",
        "Computer Science Intro", "Agricultural Science",
        "Quran (Arabic-Kinyarwanda)", "Poetry Collection", "Business Management",
        "Primary Science Workbook",
    ],
    "Beauty": [
        "Nivea Body Lotion 400ml", "Dark & Lovely Relaxer Kit",
        "Vaseline Petroleum Jelly", "Nice & Lovely Perfume", "Colgate Toothpaste",
        "Dettol Soap Bar", "Shea Butter Cream", "Hair Braiding Extensions",
        "Nail Polish Set", "Coconut Oil 500ml", "Deodorant Roll-On",
        "Face Powder Compact", "Lip Gloss Set", "Hair Clippers",
        "Baby Oil Johnson's",
    ],
    "Toys": [
        "Building Blocks Set", "Toy Car Collection", "Doll Set",
        "Board Game Ludo", "Puzzle 100pc", "Coloring Book & Crayons",
        "Toy Football", "Stuffed Animal", "Play Kitchen Set",
        'Bicycle (Kids 16")', "Scrabble Board Game", "Toy Drum",
        "Action Figures", "Card Game UNO", "Kite",
    ],
    "Food & Beverages": [
        "Inyange Milk 1L", "Skol Lager 500ml", "Urwagwa (Banana Beer)",
        "Ikivuguto (Fermented Milk)", "Isombe Mix (Cassava Leaves)",
        "Bralirwa Primus 500ml", "Akabanga Chili Oil",
        "Rwanda Mountain Tea 250g", "Cafe de Kigali Coffee 500g",
        "Fanta Citron 500ml", "Inyange Juice 1L", "Sugar 1kg", "Rice 5kg",
        "Cooking Oil 5L", "Wheat Flour 2kg", "Dried Beans 5kg",
        "Ibishyimbo (Fresh Beans) 1kg", "Bananas (Bunch)",
        "Irish Potatoes 10kg", "Tomato Paste 400g",
    ],
    "Automotive": [
        "Motorcycle Helmet", "Moto Taxi Rain Cover", "Car Battery 12V",
        "Tire 195/65R15", "Motor Oil 5W-30 4L", "Phone Mount (Moto)",
        "Jumper Cables", "Air Freshener Pack", "Wiper Blades", "Car Floor Mats",
        "Motorcycle Chain", "Spare Tire Jack", "Fuel Can 20L",
        "LED Headlight Bulbs", "Seat Covers Set",
    ],
    "Health": [
        "First Aid Kit", "Digital Thermometer", "Mosquito Repellent Spray",
        "Paracetamol 500mg Box", "Vitamin C Tablets", "Hand Sanitizer 500ml",
        "Face Masks Box 50", "Blood Pressure Monitor", "Glucose Meter Kit",
        "Antiseptic Cream", "Rehydration Salts ORS", "Multivitamin Pack",
        "Malaria Test Kit", "Cough Syrup 200ml", "Bandage Roll",
    ],
    "Office Supplies": [
        "A4 Paper Ream 500", "Ballpoint Pen Box", "Exercise Books 10-pack",
        "Calculator Casio", "Stapler & Staples", "Desk Organizer",
        "Whiteboard Marker Set", "Filing Folders Pack", "Desk Lamp LED",
        "Printer Ink Cartridge", "Notebook Hardcover A5", "Paper Clips Box",
        "Correction Fluid", "Rubber Bands Pack", "Scissors Set",
    ],
    "Jewelry": [
        "Gold-plated Necklace", "Agaseke Earrings", "Beaded Bracelet",
        "Imigongo-style Pendant", "Cowrie Shell Necklace", "Anklet Silver",
        "Wedding Band Set", "Pearl Stud Earrings", "African Bead Set",
        "Charm Bracelet", "Wooden Bangle", "Crystal Pendant",
        "Hoop Earrings Gold", "Cufflinks Set", "Ring Silver 925",
    ],
    "Pet Supplies": [
        "Dog Food 5kg", "Cat Litter 10kg", "Pet Leash & Collar", "Dog Shampoo",
        "Fish Tank 20L", "Bird Cage", "Pet Food Bowl", "Flea & Tick Spray",
        "Dog Bed Medium", "Cat Scratching Post", "Aquarium Filter",
        "Pet Carrier Bag", "Chew Toys Pack", "Rabbit Hutch", "Pet Vitamins",
    ],
    "Music": [
        "Acoustic Guitar", "Ingoma Drum", "Inanga (Zither)",
        "Umuduri (Musical Bow)", "Keyboard Piano 61-key", "Djembe Drum",
        "Guitar Strings Set", "Microphone Wireless", "Music Stand",
        "Tambourine", "Recorder Flute", "Speaker Bluetooth", "Headphone Amp",
        "Cajon Drum", "Ukulele",
    ],
    "Art & Crafts": [
        "Imigongo Art Panel", "Agaseke Basket (Peace)", "Painting Canvas Set",
        "Acrylic Paint Set", "Weaving Loom Kit", "Beadwork Supplies",
        "Wood Carving Tools", "Pottery Clay 5kg", "Sewing Machine Singer",
        "Embroidery Thread Set", "Fabric Scissors", "Crochet Hook Set",
        "Batik Dye Kit", "Sketch Pad A3", "Calligraphy Pen Set",
    ],
}

# Price ranges in RWF per category
PRICE_RANGES_RWF = {
    "Electronics": (15_000, 1_500_000),
    "Clothing": (3_000, 80_000),
    "Home & Garden": (2_000, 500_000),
    "Sports": (5_000, 150_000),
    "Books": (2_000, 25_000),
    "Beauty": (500, 30_000),
    "Toys": (1_000, 50_000),
    "Food & Beverages": (500, 30_000),
    "Automotive": (5_000, 300_000),
    "Health": (500, 50_000),
    "Office Supplies": (300, 15_000),
    "Jewelry": (5_000, 200_000),
    "Pet Supplies": (2_000, 80_000),
    "Music": (5_000, 500_000),
    "Art & Crafts": (2_000, 200_000),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_dt(start: datetime, end: datetime) -> datetime:
    """Return a random datetime between start and end."""
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta))


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def rwandan_phone() -> str:
    prefix = random.choice(["72", "73", "78", "79"])
    number = f"{random.randint(0, 9999999):07d}"
    return f"+250 {prefix}{number[0]} {number[1:4]} {number[4:]}"


def rwandan_geo() -> dict:
    city, province = random.choice(RWANDAN_CITIES_PROVINCES)
    return {"city": city, "province": province, "country": "RW"}


def rwandan_name() -> tuple[str, str]:
    return random.choice(RWANDAN_FIRST_NAMES), random.choice(RWANDAN_LAST_NAMES)


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def generate_categories() -> list[dict]:
    print(f"Generating {NUM_CATEGORIES} categories...")
    category_names = [
        "Electronics", "Clothing", "Home & Garden", "Sports", "Books",
        "Beauty", "Toys", "Food & Beverages", "Automotive", "Health",
        "Office Supplies", "Jewelry", "Pet Supplies", "Music", "Art & Crafts",
    ]
    categories = []
    for i, name in enumerate(category_names[:NUM_CATEGORIES], 1):
        cat_id = f"cat_{i:03d}"
        num_subs = random.randint(3, 6)
        subcategories = []
        for j in range(1, num_subs + 1):
            subcategories.append({
                "subcategory_id": f"sub_{i:03d}_{j:02d}",
                "name": fake.bs().title(),
                "profit_margin": round(random.uniform(0.05, 0.45), 2),
            })
        categories.append({
            "category_id": cat_id,
            "name": name,
            "subcategories": subcategories,
        })
    return categories


def generate_products(categories: list[dict]) -> list[dict]:
    print(f"Generating {NUM_PRODUCTS} products...")
    # Build lookup: category_id -> (category_name, [(cat_id, sub_id)])
    cat_id_to_name = {cat["category_id"]: cat["name"] for cat in categories}
    cat_sub_pairs = []
    for cat in categories:
        for sub in cat["subcategories"]:
            cat_sub_pairs.append((cat["category_id"], sub["subcategory_id"]))

    # Track product name usage per category to cycle through names
    cat_name_idx: dict[str, int] = defaultdict(int)

    products = []
    for i in range(1, NUM_PRODUCTS + 1):
        cat_id, sub_id = random.choice(cat_sub_pairs)
        cat_name = cat_id_to_name[cat_id]

        # Pick product name from Rwandan products list, cycling if needed
        product_names = RWANDAN_PRODUCTS.get(cat_name, ["Generic Product"])
        idx = cat_name_idx[cat_name] % len(product_names)
        product_name = product_names[idx]
        cat_name_idx[cat_name] += 1

        # Price in RWF based on category
        low, high = PRICE_RANGES_RWF.get(cat_name, (1_000, 100_000))
        base_price = round(random.uniform(low, high))
        stock = random.randint(0, 200)

        # Price history: 1-5 entries
        num_prices = random.randint(1, 5)
        price_history = []
        for _ in range(num_prices):
            ph_date = random_dt(DATE_START, DATE_END)
            variation = random.uniform(0.85, 1.15)
            price_history.append({
                "price": round(base_price * variation),
                "date": iso(ph_date),
            })
        price_history.sort(key=lambda x: x["date"])

        creation_date = random_dt(
            DATE_START - timedelta(days=365),
            DATE_START - timedelta(days=1),
        )

        products.append({
            "product_id": f"prod_{i:05d}",
            "name": product_name,
            "category_id": cat_id,
            "subcategory_id": sub_id,
            "base_price": base_price,
            "current_stock": stock,
            "is_active": stock > 0,
            "price_history": price_history,
            "creation_date": iso(creation_date),
        })
    return products


def generate_users() -> list[dict]:
    print(f"Generating {NUM_USERS} users...")
    users = []
    for i in range(1, NUM_USERS + 1):
        first, last = rwandan_name()
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@{fake.free_email_domain()}"
        reg_date = random_dt(
            DATE_START - timedelta(days=730),
            DATE_START,
        )
        last_active = random_dt(DATE_START, DATE_END)

        users.append({
            "user_id": f"user_{i:06d}",
            "name": f"{first} {last}",
            "email": email,
            "phone": rwandan_phone(),
            "geo_data": rwandan_geo(),
            "registration_date": iso(reg_date),
            "last_active": iso(last_active),
        })
    return users


def generate_sessions(
    users: list[dict], products: list[dict], categories: list[dict],
) -> tuple[list[dict], list[dict]]:
    print(f"Generating {NUM_SESSIONS} sessions...")
    user_ids = [u["user_id"] for u in users]
    product_ids = [p["product_id"] for p in products]
    category_ids = [c["category_id"] for c in categories]

    sessions = []
    transactions = []

    for _ in range(NUM_SESSIONS):
        session_id = f"sess_{secrets.token_hex(5)}"
        user_id = random.choice(user_ids)
        start = random_dt(DATE_START, DATE_END)
        duration = random.randint(30, 3600)
        end = start + timedelta(seconds=duration)

        city, province = random.choice(RWANDAN_CITIES_PROVINCES)
        geo = {
            "city": city,
            "state": province,
            "country": "RW",
            "ip_address": fake.ipv4(),
        }

        device = random.choice(DEVICE_COMBOS).copy()

        # Viewed products
        num_viewed = random.randint(1, 8)
        viewed = random.sample(product_ids, min(num_viewed, len(product_ids)))

        # Page views
        num_pages = random.randint(2, 10)
        page_views = []
        for pv_i in range(num_pages):
            pv_time = start + timedelta(seconds=int(duration * pv_i / num_pages))
            page_type = random.choice(PAGE_TYPES)
            pv = {
                "timestamp": iso(pv_time),
                "page_type": page_type,
                "product_id": random.choice(viewed) if page_type == "product" else None,
                "category_id": random.choice(category_ids) if page_type == "category" else None,
                "view_duration": random.randint(3, 120),
            }
            page_views.append(pv)

        # Cart contents (subset of viewed, 0-4 items)
        num_cart = random.randint(0, min(4, len(viewed)))
        cart_items = random.sample(viewed, num_cart)
        cart_contents = {}
        products_by_id = {p["product_id"]: p for p in products}
        for pid in cart_items:
            cart_contents[pid] = {
                "quantity": random.randint(1, 3),
                "price": products_by_id[pid]["base_price"],
            }

        # Conversion status
        if cart_contents and random.random() < 0.30:
            conversion_status = "converted"
        elif cart_contents:
            conversion_status = "abandoned"
        else:
            conversion_status = "browsing"

        session = {
            "session_id": session_id,
            "user_id": user_id,
            "start_time": iso(start),
            "end_time": iso(end),
            "duration_seconds": duration,
            "geo_data": geo,
            "device_profile": device,
            "viewed_products": viewed,
            "page_views": page_views,
            "cart_contents": cart_contents,
            "conversion_status": conversion_status,
            "referrer": random.choice(REFERRERS),
        }
        sessions.append(session)

        # Generate transaction for converted sessions
        if conversion_status == "converted":
            txn_id = f"txn_{secrets.token_hex(6)}"
            items = []
            for pid, cart_info in cart_contents.items():
                subtotal = round(cart_info["quantity"] * cart_info["price"], 2)
                items.append({
                    "product_id": pid,
                    "quantity": cart_info["quantity"],
                    "unit_price": cart_info["price"],
                    "subtotal": subtotal,
                })
            txn_subtotal = round(sum(it["subtotal"] for it in items), 2)

            # ~20% of transactions get a discount
            if random.random() < 0.20:
                discount_pct = random.uniform(0.05, 0.15)
                discount = round(txn_subtotal * discount_pct, 2)
            else:
                discount = 0.0

            transactions.append({
                "transaction_id": txn_id,
                "session_id": session_id,
                "user_id": user_id,
                "timestamp": iso(end),
                "items": items,
                "subtotal": txn_subtotal,
                "discount": discount,
                "total": round(txn_subtotal - discount, 2),
                "payment_method": random.choice(PAYMENT_METHODS),
                "status": random.choice(STATUS_CHOICES),
            })

    return sessions, transactions


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}")
    days = (DATE_END - DATE_START).days
    print(f"Date range: {DATE_START.date()} to {DATE_END.date()} ({days} days)\n")

    # Generate in dependency order
    categories = generate_categories()
    products = generate_products(categories)
    users = generate_users()
    sessions, transactions = generate_sessions(users, products, categories)

    # Write categories
    path = os.path.join(OUTPUT_DIR, "categories.json")
    with open(path, "w") as f:
        json.dump(categories, f, indent=2)
    print(f"  -> {path} ({len(categories)} categories)")

    # Write products
    path = os.path.join(OUTPUT_DIR, "products.json")
    with open(path, "w") as f:
        json.dump(products, f, indent=2)
    print(f"  -> {path} ({len(products)} products)")

    # Write users
    path = os.path.join(OUTPUT_DIR, "users.json")
    with open(path, "w") as f:
        json.dump(users, f, indent=2)
    print(f"  -> {path} ({len(users)} users)")

    # Write sessions (split across files)
    chunk_size = len(sessions) // SESSION_FILE_COUNT
    for i in range(SESSION_FILE_COUNT):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size if i < SESSION_FILE_COUNT - 1 else len(sessions)
        chunk = sessions[start_idx:end_idx]
        path = os.path.join(OUTPUT_DIR, f"sessions_{i}.json")
        with open(path, "w") as f:
            json.dump(chunk, f, indent=2)
        print(f"  -> {path} ({len(chunk)} sessions)")

    # Write transactions
    path = os.path.join(OUTPUT_DIR, "transactions.json")
    with open(path, "w") as f:
        json.dump(transactions, f, indent=2)
    print(f"  -> {path} ({len(transactions)} transactions)")

    print(f"\nDone! Generated {len(users)} users, {len(categories)} categories, "
          f"{len(products)} products, {len(sessions)} sessions, "
          f"{len(transactions)} transactions.")


if __name__ == "__main__":
    main()
