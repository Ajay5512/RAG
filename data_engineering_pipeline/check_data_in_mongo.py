from pymongo import MongoClient


# MongoDB connection settings
username = "root"
password = "root"
host = "localhost"
port = 27017
database_name = "web_scraper_db"  # Replace with your database name
collection_name = "blog_posts"  # Replace with your collection name

# Create MongoDB client
client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}/")

# Access the database
db = client[database_name]

# Access the collection
collection = db[collection_name]

# Fetch one document from the collection
document = collection.find_one()

# Print the document
if document:
    print(document)
else:
    print("No document found.")

# Close the connection
client.close()
