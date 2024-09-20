from elasticsearch import Elasticsearch
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Elasticsearch connection
es_client = Elasticsearch('http://localhost:9200')

def get_document_count(index_name: str) -> int:
    """Get the total number of documents in the Elasticsearch index."""
    result = es_client.count(index=index_name)
    return result['count']

def get_sample_document(index_name: str) -> dict:
    """Retrieve a sample document from the Elasticsearch index."""
    result = es_client.search(index=index_name, body={"query": {"match_all": {}}, "size": 1})
    if result['hits']['hits']:
        return result['hits']['hits'][0]['_source']
    return None

def main():
    index_name = "blog_posts_index"  # Make sure this matches your index name
    
    # Check if the index exists
    if not es_client.indices.exists(index=index_name):
        logging.error(f"Index '{index_name}' does not exist in Elasticsearch.")
        return

    # Get document count
    doc_count = get_document_count(index_name)
    logging.info(f"Total documents in Elasticsearch index '{index_name}': {doc_count}")

    # Get a sample document
    sample_doc = get_sample_document(index_name)
    if sample_doc:
        logging.info("Sample document from Elasticsearch:")
        print(json.dumps(sample_doc, indent=2))
    else:
        logging.info("No documents found in Elasticsearch.")

if __name__ == "__main__":
    main()