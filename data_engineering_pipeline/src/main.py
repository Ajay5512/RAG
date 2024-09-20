import logging
from bs4 import BeautifulSoup
from tqdm import tqdm
from src.scraper.url_extractor import extract_all_urls, clean_urls, get_webpage_content
from src.scraper.content_scraper import extract_blog_data
from src.db.mongo_handler import MongoHandler
from src.config import ROOT_URL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Extract URLs of all blog posts
    logging.info("Extracting blog post URLs")
    urls_list = extract_all_urls(root=ROOT_URL)
    blog_post_urls = clean_urls(urls_list)

    # Initialize MongoDB handler
    mongo_handler = MongoHandler()

    # Extract content of each blog post and save to MongoDB
    logging.info("Extracting blog post content")
    for url in tqdm(blog_post_urls):
        response = get_webpage_content(url)
        if response is None:
            logging.warning(f"Failed to fetch URL: {url}")
            continue

        soup = BeautifulSoup(response.content, "html.parser")
        blog_content = extract_blog_data(soup, url)
        
        mongo_handler.save_blog_post(blog_content)

    logging.info("Scraping and saving complete")

    # Test MongoDB connection
    total_documents = mongo_handler.count_documents()
    logging.info(f"Total documents in collection: {total_documents}")
    
    if total_documents > 0:
        sample_document = mongo_handler.get_sample_document()
        logging.info(f"Sample document: {sample_document}")

    # Close MongoDB connection
    mongo_handler.close_connection()

if __name__ == "__main__":
    main()