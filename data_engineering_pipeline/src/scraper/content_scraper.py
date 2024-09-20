from typing import Dict, List
from bs4 import BeautifulSoup
from src.utils.helpers import replace_strange_chars
from src.config import EXCLUDE_STARTSWITH
import logging

def get_meta_data(soup: BeautifulSoup) -> Dict[str, str]:
    """Extracts metadata from a blog page such as title, created date, and updated date."""
    meta_data = {}
    
    # Extract title
    title_element = soup.find("h1", class_="entry-title")
    if title_element:
        meta_data["title"] = title_element.get_text(strip=True)
    else:
        # Fallback to other possible title elements
        title_element = soup.find("title") or soup.find("h1")
        meta_data["title"] = title_element.get_text(strip=True) if title_element else "Unknown Title"
    
    # Extract dates
    time_elements = soup.find_all("time")
    if len(time_elements) >= 2:
        meta_data["created"] = time_elements[0].get("datetime", "Unknown")
        meta_data["updated"] = time_elements[1].get("datetime", "Unknown")
    elif len(time_elements) == 1:
        meta_data["created"] = time_elements[0].get("datetime", "Unknown")
        meta_data["updated"] = "Unknown"
    else:
        meta_data["created"] = "Unknown"
        meta_data["updated"] = "Unknown"
    
    logging.info(f"Extracted metadata: {meta_data}")
    return meta_data

def get_paragraphs(soup: BeautifulSoup) -> List[str]:
    """Extracts and cleans paragraphs from the blog content, excluding certain phrases."""
    paragraphs_html = soup.find_all("p", class_="p1") or soup.find_all("p")
    paragraphs_raw = [replace_strange_chars(para_html.get_text().strip()) for para_html in paragraphs_html]

    paragraphs_clean = [
        para_raw
        for para_raw in paragraphs_raw
        if para_raw and not any(para_raw.startswith(prefix) for prefix in EXCLUDE_STARTSWITH)
    ]
    logging.info(f"Extracted {len(paragraphs_clean)} clean paragraphs")
    return paragraphs_clean

def get_key_takeaways(soup: BeautifulSoup) -> List[str]:
    """Extracts key takeaways from the blog content."""
    key_takeaways_heading = soup.find("p", string="KEY TAKEAWAYS")
    if key_takeaways_heading is None:
        logging.info("No key takeaways found")
        return []

    key_takeaways_list = key_takeaways_heading.find_next("ul")
    takeaways = [replace_strange_chars(li.get_text().strip()) for li in key_takeaways_list.find_all("li")]
    logging.info(f"Extracted {len(takeaways)} key takeaways")
    return takeaways

def extract_blog_data(soup: BeautifulSoup, url: str) -> Dict:
    """Extracts all relevant blog data, including metadata, paragraphs, categories, and key takeaways."""
    blog_content = get_meta_data(soup)

    article = soup.find("article")
    if article:
        tags_raw = article.get("class", [])
        blog_content["category"] = [cat.split("-")[1] for cat in tags_raw if cat.startswith("category-")]
        blog_content["blog_tags"] = [tag.split("-")[1:] for tag in tags_raw if tag.startswith("tag-")]
        blog_content["raw_tags"] = tags_raw
    else:
        logging.warning("No article tag found, categories and tags might be missing")
        blog_content["category"] = []
        blog_content["blog_tags"] = []
        blog_content["raw_tags"] = []

    blog_content["paragraphs"] = get_paragraphs(soup)
    blog_content["key_takeaways"] = get_key_takeaways(soup)
    blog_content["url"] = url

    logging.info(f"Extracted blog data for URL: {url}")
    return blog_content