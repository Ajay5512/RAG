import logging
from typing import Dict
from pymongo import MongoClient
from src.config import MONGODB_URI, DATABASE_NAME, COLLECTION_NAME

class MongoHandler:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]
        self.collection = self.db[COLLECTION_NAME]

    def save_blog_post(self, blog_content: Dict):
        """Saves the blog content to MongoDB."""
        try:
            result = self.collection.insert_one(blog_content)
            logging.info(f"Inserted document with ID: {result.inserted_id}")
        except Exception as e:
            logging.error(f"Error saving blog post to MongoDB: {e}")

    def get_sample_document(self):
        """Retrieves a sample document from the collection."""
        return self.collection.find_one()

    def count_documents(self):
        """Counts the total number of documents in the collection."""
        return self.collection.count_documents({})

    def close_connection(self):
        """Closes the MongoDB connection."""
        self.client.close()