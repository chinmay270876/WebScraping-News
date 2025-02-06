import requests
from bs4 import BeautifulSoup
import mysql.connector
from urllib.parse import urljoin, urlparse
import random
import time
import datetime
import re
import hashlib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os  # For environment variables

# Database configuration (using environment variables)
db_config = {
    'host': os.environ.get("DB_HOST"),
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASSWORD"),
    'database': os.environ.get("DB_NAME"),
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
        exit(1) # Exit if table creation fails

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
        exit(1) # Exit if database connection fails


def generate_content_hash(content):
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def scrape(url, visited=None, website=None, article_count=0, max_articles=25):
    # ... (scrape function remains the same as the previous complete code)
    return article_count

import urllib.parse

def fetch_articles_for_newsletter(website=None):
    try:
        with mysql.connector.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT title, description, url, date, website
                    FROM news_articles
                    WHERE DATE(date) = CURDATE()
                """
                if website:
                    query += f" AND website = '{website}'"
                cursor.execute(query)
                articles = cursor.fetchall()
                return articles
    except mysql.connector.Error as e:
        print(f"Database error fetching articles: {e}")
        return []
    except Exception as e:
        print(f"A General error occurred: {e}")
        return []


def send_newsletter(articles, to_emails, website=None):
    if not articles:
        print("No new articles to send.")
        return

    from_email = os.environ.get("EMAIL_USER")  # Email from environment variable
    password = os.environ.get("EMAIL_PASSWORD") # Password from environment variable

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = ", ".join(to_emails)
    subject = f"Daily News Newsletter - {datetime.date.today()}"
    if website:
      subject += f" ({website})"
    msg['Subject'] = subject

    email_body = "<html><body>"
    if website:
      email_body += f"<h2>News from {website}</h2>"

    for title, description, url, date, website in articles:
        email_body += f"<h3>{title}</h3>"
        email_body += f"<p>{description}</p>"
        email_body += f"<p><a href='{url}'>Read More</a></p>"
        email_body += f"<p>Source: {website} | Date: {date}</p><hr>"

    email_body += "</body></html>"
    msg.attach(MIMEText(email_body, 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:  # Or your email provider's settings
            server.login(from_email, password)
            server.sendmail(from_email, to_emails, msg.as_string())
        print("Newsletter sent successfully!")
    except smtplib.SMTPException as e:
        print(f"Error sending newsletter: {e}")
    except Exception as e:
        print(f"A General error occurred: {e}")


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

    # Newsletter sending
    to_emails = [os.environ.get("RECIPIENT_EMAIL")]  # Recipient from env variable

    # Send all news:
    articles = fetch_articles_for_newsletter()
    send_newsletter(articles, to_emails)

