import requests
from bs4 import BeautifulSoup
import mysql.connector
from urllib.parse import urljoin, urlparse
import random
import time
import datetime
import re
import hashlib

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Replace with your password
    'database': 'all_news',
}

def create_table():
    try:
        with mysql.connector.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS news_articles (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        title VARCHAR(255),
                        description TEXT,
                        url VARCHAR(255) UNIQUE,
                        date DATETIME,
                        content_hash VARCHAR(32) UNIQUE,
                        website VARCHAR(50)
                    )
                """)
                conn.commit()
    except mysql.connector.Error as e:
        print(f"Database error creating table: {e}")

def store_article(title, description, url, date, content_hash, website):
    try:
        with mysql.connector.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        INSERT IGNORE INTO news_articles (title, description, url, date, content_hash, website)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (title, description, url, date, content_hash, website))
                    conn.commit()
                    print(f"Stored: {title} from {website}")
                except mysql.connector.IntegrityError:
                    print(f"Skipping duplicate (URL or Content): {url}")
                except mysql.connector.Error as e:
                    print(f"Database error inserting article: {e}")
    except mysql.connector.Error as e:
        print(f"Database connection error: {e}")

def generate_content_hash(content):
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def scrape(url, visited=None, website=None, article_count=0, max_articles=25):
    if visited is None:
        visited = set()
    if url in visited:
        return article_count

    visited.add(url)

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        time.sleep(random.uniform(1, 3))
        soup = BeautifulSoup(response.content, 'html.parser')

        title_tag = soup.find('h1') or soup.find('title')
        content_tags = soup.find_all('p')

        if title_tag and content_tags:
            title = title_tag.get_text(strip=True)
            content = "\n".join(tag.get_text(strip=True) for tag in content_tags)

            content_hash = generate_content_hash(content)
            store_article(title, content, url, datetime.datetime.now(), content_hash, website)
            article_count += 1

            if article_count >= max_articles:
                return article_count

        for a_tag in soup.find_all('a', href=True):
            article_url = urljoin(url, a_tag['href'])
            parsed_url = urlparse(article_url)
            current_parsed_url = urlparse(url)

            if parsed_url.netloc == current_parsed_url.netloc:
                article_count = scrape(article_url, visited, website, article_count, max_articles)
                if article_count >= max_articles:
                    return article_count

    except requests.exceptions.RequestException as e:
        print(f"Error scraping {url}: {e}")
    except Exception as e:
        print(f"General error scraping {url}: {e}")

    return article_count

import urllib.parse

if __name__ == "__main__":
    create_table()
    websites = {
        "chemanalyst": "https://www.chemanalyst.com/Pricing-data/carbon-black-42",
        "hindustantimes": "https://www.hindustantimes.com/",
        "economictimes": "https://economictimes.indiatimes.com/?from=mdr",
        "timesofindia": "https://timesofindia.indiatimes.com"
    }

    for website, base_url in websites.items():
        print(f"Scraping {website}...")
        scrape(base_url, website=website)
        print(f"Finished scraping {website}.")