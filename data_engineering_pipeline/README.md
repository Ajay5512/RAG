# Web Scraper Pipeline

This project is a web scraping pipeline that extracts data from a blog, saves it to MongoDB, processes the data, and indexes it in Elasticsearch.

## Prerequisites

- Python 3.8+
- Docker and Docker Compose

## Setup


1. Clone the repository:
   ```
   git clone https://github.com/yourusername/web_scraper_pipeline.git
   cd web_scraper_pipeline
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Create a `.env` file in the root directory with the following content:
   ```
   MONGO_URI=mongodb://root:root@localhost:27017/
   MONGO_DB_NAME=web_scraper_db
   MONGO_COLLECTION_NAME=blog_posts
   ```

## Running the Scraper

1. Start the MongoDB and Elasticsearch containers:
   ```
   docker-compose up -d
   ```

2. Run the scraper:
   ```
   python src/main.py
   ```

## Processing and Indexing Data

After running the scraper, you can process the data and index it in Elasticsearch:

### set the python path 

```
export PYTHONPATH=$PYTHONPATH:/workspaces/RAG/data_engineering_pipeline

```

```
python src/process_and_index.py
```

This script will:
1. Retrieve data from MongoDB
2. Process the data and generate embeddings
3. Create an Elasticsearch index
4. Index the processed data in Elasticsearch
5. Run a sample k-NN search query

## Running Tests

To run the unit tests:
```
python -m pytest tests/
```

## Project Structure

- `src/`: Contains the main source code
  - `main.py`: Entry point of the scraping application
  - `config.py`: Configuration settings
  - `scraper/`: Web scraping logic
  - `db/`: Database operations
  - `utils/`: Utility functions
  - `process_and_index.py`: Script for processing and indexing data in Elasticsearch
- `tests/`: Unit tests
- `docker-compose.yml`: Docker Compose configuration for MongoDB and Elasticsearch
- `requirements.txt`: Python dependencies

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct, and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.
