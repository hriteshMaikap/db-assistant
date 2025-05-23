import sqlite3
import random
import datetime
from decimal import Decimal

random.seed(42)

# Define sample data for Indian e-commerce context
cities = [
    ("Mumbai", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Bengaluru", "Karnataka"),
    ("Hyderabad", "Telangana"),
    ("Chennai", "Tamil Nadu"),
    ("Pune", "Maharashtra"),
    ("Ahmedabad", "Gujarat"),
    ("Kolkata", "West Bengal"),
    ("Jaipur", "Rajasthan"),
    ("Lucknow", "Uttar Pradesh")
]   

first_names = ["Aarav", "Vivaan", "Ananya", "Diya", "Arjun", "Saanvi", "Ishaan", "Aanya", "Reyansh", "Kavya",
               "Vihaan", "Aadhya", "Ayaan", "Pihu", "Krishna", "Myra", "Aryan", "Sara", "Atharv", "Riya",
               "Aadit", "Ira", "Kabir", "Tara", "Shivansh", "Zara", "Advait", "Nora", "Sai", "Aria"]

last_names = ["Sharma", "Patel", "Reddy", "Singh", "Gupta", "Kumar", "Yadav", "Shah", "Jain", "Agarwal",
              "Mehta", "Verma", "Mishra", "Srivastava", "Pandey", "Tiwari", "Joshi", "Nair", "Iyer", "Rao"]

categories = ["Electronics", "Clothing & Fashion", "Home & Kitchen", "Books & Media", "Sports & Fitness", 
              "Beauty & Personal Care", "Toys & Games", "Automotive"]

product_prefixes = ["Premium", "Ultra", "Smart", "Pro", "Elite", "Max", "Super", "Deluxe", "Advanced", "Classic"]

def random_date(start, end):
    return start + datetime.timedelta(days=random.randint(0, (end-start).days))

def random_email_domain():
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "rediffmail.com"]
    return random.choice(domains)

# Create or connect to sqlite file
conn = sqlite3.connect('ecommerce_platform.db')
c = conn.cursor()

# Drop tables if they exist
for tbl in ['order_items', 'orders', 'products', 'customers']:
    c.execute(f"DROP TABLE IF EXISTS {tbl}")

print("Creating tables...")

# Create customers table
c.execute('''CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT,
    city TEXT,
    state TEXT,
    registration_date TEXT,
    is_premium BOOLEAN DEFAULT 0,
    total_orders INTEGER DEFAULT 0
)''')

# Create products table
c.execute('''CREATE TABLE products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    category TEXT,
    price DECIMAL(10,2),
    stock_quantity INTEGER,
    rating DECIMAL(3,2),
    created_date TEXT,
    is_active BOOLEAN DEFAULT 1
)''')

# Create orders table
c.execute('''CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER,
    order_date TEXT,
    shipping_city TEXT,
    shipping_state TEXT,
    order_status TEXT,
    payment_method TEXT,
    total_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2) DEFAULT 0,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
)''')

# Create order_items table
c.execute('''CREATE TABLE order_items (
    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_price DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
)''')

print("Inserting sample data...")

# Insert customers (100 records)
customers_data = []
for i in range(100):
    fn = random.choice(first_names)
    ln = random.choice(last_names)
    city, state = random.choice(cities)
    email = f"{fn.lower()}.{ln.lower()}{i+1}@{random_email_domain()}"
    phone = f"+91{random.randint(7000000000, 9999999999)}"
    reg_date = random_date(datetime.date(2022, 1, 1), datetime.date(2024, 12, 31)).isoformat()
    is_premium = random.choice([0, 0, 0, 1])  # 25% premium customers
    total_orders = random.randint(0, 15)
    
    customers_data.append((fn, ln, email, phone, city, state, reg_date, is_premium, total_orders))

c.executemany("INSERT INTO customers (first_name, last_name, email, phone, city, state, registration_date, is_premium, total_orders) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", customers_data)

# Insert products (150 records)
products_data = []
for i in range(150):
    category = random.choice(categories)
    prefix = random.choice(product_prefixes)
    
    # Generate category-specific product names
    if category == "Electronics":
        base_names = ["Smartphone", "Laptop", "Headphones", "Tablet", "Smart Watch", "Speaker", "Camera"]
    elif category == "Clothing & Fashion":
        base_names = ["T-Shirt", "Jeans", "Dress", "Jacket", "Shoes", "Handbag", "Sunglasses"]
    elif category == "Home & Kitchen":
        base_names = ["Mixer", "Pressure Cooker", "Air Fryer", "Microwave", "Blender", "Toaster", "Cookware Set"]
    elif category == "Books & Media":
        base_names = ["Novel", "Textbook", "Biography", "Cookbook", "Self-Help", "Comic", "Magazine"]
    elif category == "Sports & Fitness":
        base_names = ["Cricket Bat", "Football", "Yoga Mat", "Dumbbells", "Treadmill", "Bicycle", "Tennis Racket"]
    elif category == "Beauty & Personal Care":
        base_names = ["Face Cream", "Shampoo", "Perfume", "Lipstick", "Hair Oil", "Body Lotion", "Face Wash"]
    elif category == "Toys & Games":
        base_names = ["Board Game", "Action Figure", "Puzzle", "Remote Car", "Building Blocks", "Doll", "Video Game"]
    else:  # Automotive
        base_names = ["Car Care Kit", "Tire", "Battery", "Engine Oil", "Car Cover", "GPS Device", "Dash Cam"]
    
    product_name = f"{prefix} {random.choice(base_names)} {i+1}"
    
    # Price ranges based on category
    if category == "Electronics":
        price = random.randint(5000, 80000)
    elif category == "Clothing & Fashion":
        price = random.randint(500, 8000)
    elif category == "Home & Kitchen":
        price = random.randint(1000, 25000)
    elif category == "Automotive":
        price = random.randint(2000, 50000)
    else:
        price = random.randint(200, 5000)
    
    stock = random.randint(0, 200)
    rating = round(random.uniform(3.0, 5.0), 1)
    created_date = random_date(datetime.date(2023, 1, 1), datetime.date(2024, 12, 31)).isoformat()
    is_active = random.choice([1, 1, 1, 1, 0])  # 80% active products
    
    products_data.append((product_name, category, price, stock, rating, created_date, is_active))

c.executemany("INSERT INTO products (product_name, category, price, stock_quantity, rating, created_date, is_active) VALUES (?, ?, ?, ?, ?, ?, ?)", products_data)

# Insert orders (200 records)
orders_data = []
payment_methods = ["Credit Card", "Debit Card", "UPI", "Net Banking", "Cash on Delivery", "Wallet"]
order_statuses = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"]

for i in range(200):
    customer_id = random.randint(1, 100)
    order_date = random_date(datetime.date(2023, 6, 1), datetime.date(2025, 1, 31)).isoformat()
    shipping_city, shipping_state = random.choice(cities)
    status = random.choice(order_statuses)
    payment_method = random.choice(payment_methods)
    discount = random.choice([0, 0, 0, 100, 200, 500, 1000])  # Most orders have no discount
    
    orders_data.append((customer_id, order_date, shipping_city, shipping_state, status, payment_method, 0, discount))

c.executemany("INSERT INTO orders (customer_id, order_date, shipping_city, shipping_state, order_status, payment_method, total_amount, discount_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", orders_data)

# Insert order_items and update order totals (400+ records)
print("Inserting order items and calculating totals...")

for order_id in range(1, 201):
    # Each order has 1-4 items
    num_items = random.randint(1, 4)
    order_total = 0
    
    for _ in range(num_items):
        product_id = random.randint(1, 150)
        quantity = random.randint(1, 3)
        
        # Get product price
        c.execute("SELECT price FROM products WHERE product_id = ?", (product_id,))
        unit_price = c.fetchone()[0]
        total_price = unit_price * quantity
        order_total += total_price
        
        c.execute("INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price) VALUES (?, ?, ?, ?, ?)",
                 (order_id, product_id, quantity, unit_price, total_price))
    
    # Update order total (subtract discount)
    c.execute("SELECT discount_amount FROM orders WHERE order_id = ?", (order_id,))
    discount = c.fetchone()[0]
    final_total = max(0, order_total - discount)
    
    c.execute("UPDATE orders SET total_amount = ? WHERE order_id = ?", (final_total, order_id))

# Commit and close
conn.commit()

# Print some statistics
print("\n=== Database Statistics ===")
c.execute("SELECT COUNT(*) FROM customers")
print(f"Customers: {c.fetchone()[0]}")

c.execute("SELECT COUNT(*) FROM products")
print(f"Products: {c.fetchone()[0]}")

c.execute("SELECT COUNT(*) FROM orders")
print(f"Orders: {c.fetchone()[0]}")

c.execute("SELECT COUNT(*) FROM order_items")
print(f"Order Items: {c.fetchone()[0]}")

c.execute("SELECT category, COUNT(*) FROM products GROUP BY category")
print("\nProducts by Category:")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]}")

c.execute("SELECT order_status, COUNT(*) FROM orders GROUP BY order_status")
print("\nOrders by Status:")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]}")

conn.close()
print(f"\nDatabase 'ecommerce_platform.db' created successfully!")
print("Tables: customers, products, orders, order_items")