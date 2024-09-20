import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv('MONGODB_URI')
DATABASE_NAME = os.getenv('DATABASE_NAME')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')
ROOT_URL = os.getenv('ROOT_URL')

REPLACEMENTS = {
    """: "'",
    """: "'",
    "'": "'",
    "'": "'",
    "…": "...",
    "—": "-",
    "\u00a0": " ",
}

EXCLUDE_STARTSWITH = [
    "Written By",
    "Image Credit",
    "In health",
    "Michael Greger",
    "-Michael Greger",
    "PS:",
    "A founding member",
    "Subscribe",
    "Catch up",
    "Charity ID",
    "We  our volunteers!",
    "Interested in learning more about",
    "Check out",
    "For more on",
]

USER_AGENT = "Mozilla/5.0"
REQUEST_TIMEOUT = 10
WAIT_TIME = 0.2