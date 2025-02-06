import requests
from bs4 import BeautifulSoup
import mysql.connector
from urllib.parse import urljoin
import random
import time
import datetime
import re

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  
    'database': 'carbonblack',  
}

def create_table():
    try:
        with mysql.connector.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS business (  
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        title VARCHAR(255),
                        description TEXT,
                        category VARCHAR(255),
                        date DATETIME
                    )
                """)
                conn.commit()
    except mysql.connector.Error as e:
        print(f"Database error creating table: {e}")

def store_article(title, description, category, date):
    try:
        with mysql.connector.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        INSERT IGNORE INTO business (title, description, category, date)
                        VALUES (%s, %s, %s, %s)
                    """, (title, description, category, date))
                    conn.commit()
                    print(f"Stored: {title}")
                except mysql.connector.IntegrityError:
                    print(f"Skipping duplicate: {url}")
                except mysql.connector.Error as e:
                    print(f"Database error inserting article: {e}")
    except mysql.connector.Error as e:
        print(f"Database connection error: {e}")

def scrape(url, visited=None):
    if visited is None:
        visited = set()
    if url in visited:
        return

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

            # Keyword Check (Case-Insensitive)
            if re.search(r"\bcarbon\b", content, re.IGNORECASE):  
                store_article(title, content, url, datetime.datetime.now())
            else:
                print(f"Skipping: '{title}' - Does not contain keyword 'carbon'")


        for a_tag in soup.find_all('a', href=True):
            article_url = urljoin(url, a_tag['href'])
            if 'chemanalyst.com' in article_url:  
                scrape(article_url, visited)

    except requests.exceptions.RequestException as e:
        print(f"Error scraping {url}: {e}")
    except Exception as e:
        print(f"General error scraping {url}: {e}")

if __name__ == "__main__":
    create_table()
    start_url = "https://www.chemanalyst.com/Pricing-data/carbon-black-42"  
    scrape(start_url)