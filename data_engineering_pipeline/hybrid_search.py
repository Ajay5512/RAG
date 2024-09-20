import logging
from typing import List, Dict, Any
import pandas as pd
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection
mongo_client = MongoClient('mongodb://root:root@localhost:27017/')
mongo_db = mongo_client['web_scraper_db']
mongo_collection = mongo_db['blog_posts']

# Elasticsearch connection
es_client = Elasticsearch('http://localhost:9200')

# Load SentenceTransformer model
model = SentenceTransformer("all-mpnet-base-v2")

def get_mongodb_data() -> List[Dict[str, Any]]:
    """Retrieve data from MongoDB."""
    logging.info("Retrieving data from MongoDB")
    return list(mongo_collection.find())

def process_mongodb_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process MongoDB data and create a DataFrame."""
    logging.info("Processing MongoDB data")
    processed_data = []
    for doc in tqdm(data, desc="Processing documents"):
        combined_text = " ".join(doc.get('paragraphs', []) + doc.get('key_takeaways', []))
        title_embedding = model.encode(doc['title']).tolist()
        combined_text_embedding = model.encode(combined_text).tolist()
        processed_data.append({
            'url': doc['url'],
            'title': doc['title'],
            'combined_text': combined_text,
            'title_vector': title_embedding,
            'combined_text_vector': combined_text_embedding,
            'blog_tags': ", ".join([" ".join(tag) for tag in doc.get('blog_tags', [])]),
            'category': ", ".join(doc.get('category', [])),
            'created': doc.get('created'),
            'updated': doc.get('updated')
        })
    return pd.DataFrame(processed_data)

def create_elasticsearch_index(index_name: str):
    """Create Elasticsearch index with specified mappings."""
    index_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "url": {"type": "keyword"},
                "title": {"type": "text"},
                "combined_text": {"type": "text"},
                "title_vector": {
                    "type": "dense_vector",
                    "dims": 768,
                    "index": True,
                    "similarity": "cosine"
                },
                "combined_text_vector": {
                    "type": "dense_vector",
                    "dims": 768,
                    "index": True,
                    "similarity": "cosine"
                },
                "blog_tags": {"type": "keyword"},
                "category": {"type": "keyword"},
                "created": {"type": "date"},
                "updated": {"type": "date"}
            }
        }
    }
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name, body=index_settings)
        logging.info(f"Created Elasticsearch index: {index_name}")
    else:
        logging.info(f"Elasticsearch index {index_name} already exists")

def index_to_elasticsearch(df: pd.DataFrame, index_name: str):
    """Index data to Elasticsearch."""
    logging.info("Indexing data to Elasticsearch")
    actions = [
        {
            "_index": index_name,
            "_source": {
                "url": row['url'],
                "title": row['title'],
                "combined_text": row['combined_text'],
                "title_vector": row['title_vector'],
                "combined_text_vector": row['combined_text_vector'],
                "blog_tags": row['blog_tags'],
                "category": row['category'],
                "created": row['created'],
                "updated": row['updated']
            }
        }
        for _, row in df.iterrows()
    ]
    helpers.bulk(es_client, actions)
    logging.info(f"Indexed {len(actions)} documents to Elasticsearch")

def run_hybrid_search(query: str, index_name: str, k: int = 5):
    """Run hybrid search in Elasticsearch based on user input."""
    query_vector = model.encode(query).tolist()
    
    knn_query = {
        "field": "combined_text_vector",
        "query_vector": query_vector,
        "k": k,
        "num_candidates": 10000,
        "boost": 0.5,
    }
    
    keyword_query = {
        "bool": {
            "must": {
                "multi_match": {
                    "query": query,
                    "fields": ["combined_text^3", "title", "blog_tags"],
                    "type": "best_fields",
                    "boost": 0.5,
                }
            }
        }
    }
    
    response = es_client.search(
        index=index_name,
        query=keyword_query,
        knn=knn_query,
        size=k
    )
    
    return response["hits"]["hits"]

def main():
    index_name = "blog_posts_index"
    
    # Get and process MongoDB data
    mongo_data = get_mongodb_data()
    df = process_mongodb_data(mongo_data)
    
    # Create Elasticsearch index and index data
    create_elasticsearch_index(index_name)
    index_to_elasticsearch(df, index_name)
    
    # Example hybrid search
    query = "healthier salt substitutes"
    search_results = run_hybrid_search(query, index_name)
    
    logging.info(f"Top 5 results for query '{query}':")
    for hit in search_results:
        logging.info(f"Title: {hit['_source']['title']}")
        logging.info(f"URL: {hit['_source']['url']}")
        logging.info(f"Score: {hit['_score']}")
        logging.info(f"Combined Text: {hit['_source']['combined_text'][:200]}...")
        logging.info("---")

if __name__ == "__main__":
    main()