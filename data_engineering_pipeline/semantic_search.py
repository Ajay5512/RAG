from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Elasticsearch connection
es_client = Elasticsearch('http://localhost:9200')

# Load SentenceTransformer model
model = SentenceTransformer("all-mpnet-base-v2")

def run_semantic_search(query: str, index_name: str, k: int = 5):
    """Run semantic search in Elasticsearch based on user input."""
    query_vector = model.encode(query).tolist()
    search_query = {
        "size": k,
        "query": {
            "script_score": {
                "query": {"match_all": {}},
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                    "params": {"query_vector": query_vector}
                }
            }
        },
        "_source": ["url", "title", "combined_text"]  # Include combined_text in the returned fields
    }
    results = es_client.search(index=index_name, body=search_query)
    return results['hits']['hits']

def main():
    index_name = "blog_posts_index"  # Make sure this matches your index name
    
    while True:
        query = input("Enter your search query (or 'quit' to exit): ")
        if query.lower() == 'quit':
            break
        
        results = run_semantic_search(query, index_name)
        
        print(f"\nTop 5 results for query '{query}':")
        for hit in results:
            print(f"Title: {hit['_source'].get('title', 'N/A')}")
            print(f"URL: {hit['_source'].get('url', 'N/A')}")
            print(f"Score: {hit['_score']}")
            print(f"Combined Text: {hit['_source'].get('combined_text', 'N/A')[:500]}...")  # Display first 500 characters
            print("---")

if __name__ == "__main__":
    main()