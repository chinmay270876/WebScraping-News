import requests
from bs4 import BeautifulSoup
import mysql.connector
from urllib.parse import urljoin
import random
import time
import datetime

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Replace with your actual password
    'database': 'economictimes',
}

# Create MySQL Table
def create_table():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wealth (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            description TEXT,
            category VARCHAR(50),
            time DATETIME
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

# Store Article in Database
def store_article(title, description, category, time):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT IGNORE INTO wealth (title, description, category, time)
            VALUES (%s, %s, %s, %s)
        """, (title, description, category, time))
        conn.commit()
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()

# Check if the URL belongs to a specific category
def is_target_category(category_segment):
    category_keywords = ["sports", "business", "entertainment", "technology"]
    return category_segment.lower() in category_keywords

# Recursive Scraper
def scrape(url, visited=set()):
    if url in visited:
        return
    visited.add(url)

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        time.sleep(random.uniform(1, 3))
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract article content
        title_tag = soup.find('h1')
        content_tags = soup.find_all('p')
        if title_tag and content_tags:
            title = title_tag.get_text(strip=True)
            content = "\n".join(tag.get_text(strip=True) for tag in content_tags)
            category = extract_category_from_url(url)
            if title and content:
                print(f"Storing article: {title}")
                store_article(title, content, category, datetime.datetime.now())

        # Find and scrape linked articles
        for a_tag in soup.find_all('a', href=True):
            article_url = urljoin(url, a_tag['href'])
            if 'economictimes.indiatimes.com' in article_url:  # Restrict scraping to Economic Times domain
                scrape(article_url, visited)

    except Exception as e:
        print(f"Error scraping {url}: {e}")

# Extract category from URL
def extract_category_from_url(url):
    category_segments = url.split("/")
    if len(category_segments) > 3:
        possible_category = category_segments[3]
        if is_target_category(possible_category):
            return possible_category
    return "Other"

if __name__ == "__main__":
    create_table()
    start_url = "https://economictimes.indiatimes.com/?from=mdr"
    scrape(start_url)