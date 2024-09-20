import logging
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Elasticsearch connection
es_client = Elasticsearch('http://localhost:9200')

# Load SentenceTransformer model
model = SentenceTransformer("all-mpnet-base-v2")

def run_hybrid_search(query: str, index_name: str, k: int = 5):
    """Run hybrid search in Elasticsearch using RRF to combine full-text and kNN results."""
    query_vector = model.encode(query).tolist()
    
    search_body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["combined_text^3", "title", "blog_tags"],
                "type": "best_fields"
            }
        },
        "knn": {
            "field": "combined_text_vector",
            "query_vector": query_vector,
            "k": k,
            "num_candidates": 100
        },
        "rank": {
            "rrf": {}
        },
        "size": k
    }
    
    try:
        response = es_client.search(index=index_name, body=search_body)
        return response["hits"]["hits"]
    except Exception as e:
        logging.error(f"Search error: {str(e)}")
        return []

def main():
    index_name = "blog_posts_index"  # Make sure this matches your actual index name
    
    while True:
        query = input("Enter your search query (or 'quit' to exit): ")
        if query.lower() == 'quit':
            break
        
        search_results = run_hybrid_search(query, index_name, k=5)
        
        if search_results:
            logging.info(f"Top {len(search_results)} results for query '{query}':")
            for hit in search_results:
                logging.info(f"Title: {hit['_source'].get('title', 'N/A')}")
                logging.info(f"URL: {hit['_source'].get('url', 'N/A')}")
                logging.info(f"Score: {hit['_score']}")
                combined_text = hit['_source'].get('combined_text', '')
                logging.info(f"Combined Text: {combined_text[:200]}...")
                logging.info("---")
        else:
            logging.info("No results found or an error occurred.")
    
    logging.info("Search session ended.")

if __name__ == "__main__":
    main()