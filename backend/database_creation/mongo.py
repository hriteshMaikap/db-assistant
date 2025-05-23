from pymongo import MongoClient
import random
from datetime import datetime, timedelta
import uuid

random.seed(42)

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ecommerce_analytics']

# Drop collections if they already exist
db.user_sessions.drop()
db.product_reviews.drop()
db.marketing_campaigns.drop()

print("Creating MongoDB collections for e-commerce analytics...")

# Generate user sessions data (behavioral analytics)
print("Inserting user sessions data...")

session_data = []
devices = ["Desktop", "Mobile", "Tablet"]
browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
traffic_sources = ["Direct", "Google Search", "Facebook", "Instagram", "Email", "Affiliate", "Paid Ads"]
page_types = ["Homepage", "Product Page", "Category Page", "Cart", "Checkout", "Account", "Search Results"]

# Generate 300 user sessions
for i in range(300):
    # Random customer_id that matches SQL database (1-100)
    customer_id = random.randint(1, 100) if random.random() > 0.3 else None  # 30% anonymous sessions
    
    session_start = datetime(2024, 1, 1) + timedelta(
        days=random.randint(0, 365),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    
    session_duration = random.randint(30, 1800)  # 30 seconds to 30 minutes
    pages_viewed = random.randint(1, 15)
    
    # Generate page views for this session
    page_views = []
    for j in range(pages_viewed):
        page_views.append({
            "page_type": random.choice(page_types),
            "page_url": f"/page_{random.randint(1, 100)}",
            "time_spent": random.randint(10, 300),
            "timestamp": session_start + timedelta(seconds=random.randint(0, session_duration))
        })
    
    # Cart actions during session
    cart_actions = []
    if random.random() > 0.6:  # 40% of sessions have cart activity
        num_actions = random.randint(1, 5)
        for k in range(num_actions):
            cart_actions.append({
                "action": random.choice(["add_to_cart", "remove_from_cart", "update_quantity"]),
                "product_id": random.randint(1, 150),  # Matches SQL product IDs
                "timestamp": session_start + timedelta(seconds=random.randint(0, session_duration))
            })
    
    session_data.append({
        "_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "session_start": session_start,
        "session_duration_seconds": session_duration,
        "device_type": random.choice(devices),
        "browser": random.choice(browsers),
        "traffic_source": random.choice(traffic_sources),
        "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
        "location": {
            "city": random.choice(["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Pune", "Ahmedabad"]),
            "country": "India"
        },
        "pages_viewed": pages_viewed,
        "page_views": page_views,
        "cart_actions": cart_actions,
        "converted_to_order": random.choice([True, False]),
        "bounce_rate": 1 if pages_viewed == 1 else 0
    })

db.user_sessions.insert_many(session_data)

# Generate product reviews data
print("Inserting product reviews data...")

review_data = []
review_titles = [
    "Excellent product!", "Great value for money", "Highly recommended", "Amazing quality",
    "Perfect for daily use", "Good but could be better", "Not as expected", "Outstanding performance",
    "Worth every penny", "Decent quality", "Fantastic experience", "Good purchase",
    "Could be improved", "Satisfied with purchase", "Exceeded expectations"
]

review_texts = [
    "This product exceeded my expectations. Great build quality and fast delivery.",
    "Good value for the price. Works as described and arrived on time.",
    "Quality could be better but overall satisfied with the purchase.",
    "Excellent product! Highly recommend to others. Five stars!",
    "Fast shipping and good packaging. Product works perfectly.",
    "Not the best quality but acceptable for the price range.",
    "Amazing product with great features. Will buy again!",
    "Good customer service and quick resolution of issues.",
    "Product matches the description. Happy with the purchase.",
    "Could have better build quality but serves the purpose well."
]

# Generate 250 reviews
for i in range(250):
    product_id = random.randint(1, 150)  # Matches SQL product IDs
    customer_id = random.randint(1, 100)  # Matches SQL customer IDs
    
    rating = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 20, 35, 30])[0]  # Skewed toward higher ratings
    
    review_date = datetime(2023, 6, 1) + timedelta(days=random.randint(0, 600))
    
    # Helpful votes (some reviews get more votes)
    helpful_votes = random.choices([0, 1, 2, 3, 5, 8, 12, 20], weights=[30, 25, 20, 10, 8, 4, 2, 1])[0]
    
    review_data.append({
        "_id": str(uuid.uuid4()),
        "product_id": product_id,
        "customer_id": customer_id,
        "rating": rating,
        "review_title": random.choice(review_titles),
        "review_text": random.choice(review_texts),
        "review_date": review_date,
        "verified_purchase": random.choice([True, True, True, False]),  # 75% verified
        "helpful_votes": helpful_votes,
        "total_votes": helpful_votes + random.randint(0, 5),
        "sentiment": "positive" if rating >= 4 else "negative" if rating <= 2 else "neutral",
        "image_urls": [f"review_image_{random.randint(1, 100)}.jpg"] if random.random() > 0.8 else []
    })

db.product_reviews.insert_many(review_data)

# Generate marketing campaigns data
print("Inserting marketing campaigns data...")

campaign_data = []
campaign_names = [
    "Summer Sale 2024", "Diwali Mega Sale", "New Year Bonanza", "Electronics Week", 
    "Fashion Festival", "Home Appliance Sale", "Back to School", "Monsoon Offers",
    "Republic Day Sale", "Independence Day Special", "Holi Colors Sale", "Winter Collection"
]

campaign_types = ["Email", "Social Media", "Google Ads", "Display Ads", "Influencer", "SMS"]
campaign_objectives = ["Brand Awareness", "Lead Generation", "Sales Conversion", "Customer Retention"]

# Generate 50 campaigns
for i in range(50):
    campaign_start = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))
    campaign_duration = random.randint(3, 30)  # 3 to 30 days
    
    budget = random.randint(10000, 500000)  # Budget in INR
    spent = random.randint(int(budget * 0.7), budget)  # 70-100% of budget spent
    
    # Campaign performance metrics
    impressions = random.randint(10000, 1000000)
    clicks = random.randint(int(impressions * 0.01), int(impressions * 0.1))  # 1-10% CTR
    conversions = random.randint(int(clicks * 0.01), int(clicks * 0.15))  # 1-15% conversion rate
    
    # Target demographics
    target_age_groups = random.sample(["18-25", "26-35", "36-45", "46-55", "55+"], random.randint(1, 3))
    target_cities = random.sample(["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Pune"], random.randint(2, 4))
    
    campaign_data.append({
        "_id": str(uuid.uuid4()),
        "campaign_name": f"{random.choice(campaign_names)} - {i+1}",
        "campaign_type": random.choice(campaign_types),
        "objective": random.choice(campaign_objectives),
        "start_date": campaign_start,
        "end_date": campaign_start + timedelta(days=campaign_duration),
        "status": random.choice(["Active", "Completed", "Paused", "Draft"]),
        "budget": {
            "allocated": budget,
            "spent": spent,
            "currency": "INR"
        },
        "targeting": {
            "age_groups": target_age_groups,
            "cities": target_cities,
            "interests": random.sample(["Technology", "Fashion", "Sports", "Travel", "Food", "Entertainment"], random.randint(2, 4))
        },
        "performance": {
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
            "ctr": round(clicks / impressions * 100, 2),
            "conversion_rate": round(conversions / clicks * 100, 2) if clicks > 0 else 0,
            "cost_per_click": round(spent / clicks, 2) if clicks > 0 else 0,
            "cost_per_conversion": round(spent / conversions, 2) if conversions > 0 else 0
        },
        "creative_assets": [
            {
                "type": "image",
                "url": f"campaign_image_{random.randint(1, 20)}.jpg",
                "size": random.choice(["banner", "square", "story"])
            },
            {
                "type": "text",
                "headline": f"Amazing deals on {random.choice(['Electronics', 'Fashion', 'Home'])}!",
                "description": "Don't miss out on incredible savings!"
            }
        ],
        "attributed_orders": random.randint(0, conversions),
        "revenue_generated": random.randint(spent, spent * 5),  # ROI varies
        "customer_acquisition_cost": round(spent / conversions, 2) if conversions > 0 else 0
    })

db.marketing_campaigns.insert_many(campaign_data)

# Print collection statistics
print("\n=== MongoDB Collections Statistics ===")

print(f"User Sessions: {db.user_sessions.count_documents({})}")
print(f"Product Reviews: {db.product_reviews.count_documents({})}")
print(f"Marketing Campaigns: {db.marketing_campaigns.count_documents({})}")

# Some additional insights
print(f"\nSessions with cart activity: {db.user_sessions.count_documents({'cart_actions': {'$ne': []}})}")
print(f"Sessions that converted to orders: {db.user_sessions.count_documents({'converted_to_order': True})}")

avg_rating = list(db.product_reviews.aggregate([
    {"$group": {"_id": None, "avg_rating": {"$avg": "$rating"}}}
]))[0]["avg_rating"]
print(f"Average product rating: {avg_rating:.2f}")

active_campaigns = db.marketing_campaigns.count_documents({"status": "Active"})
print(f"Active marketing campaigns: {active_campaigns}")

# Show sample documents
print(f"\n=== Sample Documents ===")
print("Sample User Session:")
sample_session = db.user_sessions.find_one()
print(f"  Customer ID: {sample_session.get('customer_id', 'Anonymous')}")
print(f"  Device: {sample_session['device_type']}")
print(f"  Pages viewed: {sample_session['pages_viewed']}")
print(f"  Duration: {sample_session['session_duration_seconds']} seconds")

print(f"\nSample Product Review:")
sample_review = db.product_reviews.find_one()
print(f"  Product ID: {sample_review['product_id']}")
print(f"  Rating: {sample_review['rating']}/5")
print(f"  Title: {sample_review['review_title']}")

print(f"\nSample Marketing Campaign:")
sample_campaign = db.marketing_campaigns.find_one()
print(f"  Name: {sample_campaign['campaign_name']}")
print(f"  Type: {sample_campaign['campaign_type']}")
print(f"  Budget: ₹{sample_campaign['budget']['allocated']:,}")
print(f"  CTR: {sample_campaign['performance']['ctr']:.2f}%")

client.close()
print(f"\nMongoDB database 'ecommerce_analytics' created successfully!")
print("Collections: user_sessions, product_reviews, marketing_campaigns")