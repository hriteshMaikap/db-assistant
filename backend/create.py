import sqlite3
import random
random.seed(42)
import datetime

# Define sample data for Indian context
cities = [
    ("Mumbai", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Bengaluru", "Karnataka"),
    ("Hyderabad", "Telangana"),
    ("Chennai", "Tamil Nadu")
]
first_names = ["Aarav", "Vivaan", "Ananya", "Diya", "Arjun"]
last_names = ["Sharma", "Patel", "Reddy", "Singh", "Gupta"]
categories = ["Smartphones", "Laptops", "Clothing", "Home & Kitchen", "Books"]

def random_date(start, end):
    return start + datetime.timedelta(days=random.randint(0, (end-start).days))

# Create or connect to sqlite file
conn = sqlite3.connect('ecommerce_test.db')
c = conn.cursor()

# Drop tables if they exist
for tbl in ['customers', 'products', 'orders', 'order_items', 'categories']:
    c.execute(f"DROP TABLE IF EXISTS {tbl}")

# Create tables
c.execute('''CREATE TABLE customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT,
    last_name TEXT,
    city TEXT,
    state TEXT,
    email TEXT UNIQUE,
    registration_date TEXT
)''')

c.execute('''CREATE TABLE categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
)''')

c.execute('''CREATE TABLE products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    category_id INTEGER,
    price INTEGER,
    stock INTEGER,
    FOREIGN KEY (category_id) REFERENCES categories(id)
)''')

c.execute('''CREATE TABLE orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER,
    order_date TEXT,
    shipping_city TEXT,
    status TEXT,
    total_amount INTEGER,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
)''')

c.execute('''CREATE TABLE order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price INTEGER,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
)''')

# Insert categories
for cat in categories:
    c.execute("INSERT INTO categories (name) VALUES (?)", (cat,))

# Insert customers
for i in range(30):
    fn = random.choice(first_names)
    ln = random.choice(last_names)
    city, state = random.choice(cities)
    email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
    reg_date = random_date(datetime.date(2022,1,1), datetime.date(2025,5,1)).isoformat()
    c.execute("INSERT INTO customers (first_name, last_name, city, state, email, registration_date) VALUES (?, ?, ?, ?, ?, ?)",
              (fn, ln, city, state, email, reg_date))

# Insert products
for i in range(50):
    prod_name = f"{random.choice(['Super', 'Ultra', 'Mega', 'Eco', 'Smart'])} {random.choice(categories)} {i+1}"
    price = random.randint(1000, 80000)
    stock = random.randint(10, 100)
    category_id = random.randint(1, len(categories))
    c.execute("INSERT INTO products (name, category_id, price, stock) VALUES (?, ?, ?, ?)",
              (prod_name, category_id, price, stock))

# Insert orders and order_items
for i in range(100):
    customer_id = random.randint(1, 30)
    order_date = random_date(datetime.date(2023,1,1), datetime.date(2025,5,1)).isoformat()
    shipping_city, _ = random.choice(cities)
    status = random.choice(['processing', 'shipped', 'delivered', 'cancelled'])
    total_amount = 0
    # Create the order
    c.execute("INSERT INTO orders (customer_id, order_date, shipping_city, status, total_amount) VALUES (?, ?, ?, ?, ?)",
              (customer_id, order_date, shipping_city, status, 0))
    order_id = c.lastrowid
    # Add 1-3 items per order
    for _ in range(random.randint(1,3)):
        product_id = random.randint(1, 50)
        quantity = random.randint(1, 4)
        c.execute("SELECT price FROM products WHERE id=?", (product_id,))
        price = c.fetchone()[0]
        c.execute("INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (?, ?, ?, ?)",
                  (order_id, product_id, quantity, price))
        total_amount += price * quantity
    # Update order with total
    c.execute("UPDATE orders SET total_amount=? WHERE id=?", (total_amount, order_id))

conn.commit()
conn.close()
print("Database ecommerce_test.db created with tables and sample data.")