import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


def adjust_url_for_pagination(base_url, page_number):
    """Adjust the URL for pagination."""
    url_parts = list(urlparse(base_url))
    query_params = parse_qs(url_parts[4])
    query_params['page'] = [str(page_number)]
    url_parts[4] = urlencode(query_params, doseq=True)
    return urlunparse(url_parts)


def fetch_listings_from_page(base_url, page_number, headers):
    """Fetch property listings from a specific page."""
    page_url = adjust_url_for_pagination(base_url, page_number)
    response = requests.get(page_url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve page {page_number}")
        return None
    soup = BeautifulSoup(response.content, "html.parser")
    next_data_script = soup.find("script", {"id": "__NEXT_DATA__"})
    if next_data_script:
        json_content = next_data_script.string
        data = json.loads(json_content)
        page_props = data["props"].get("pageProps", {})
        search_result = page_props.get("searchResult", {})
        listings = search_result.get("listings", [])
        return listings
    else:
        print(f"No listings found on page {page_number}")
        return None


def scrape_properties(base_url):
    """Main function to scrape property data."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }

    page_number = 1
    retry_count = 0
    all_listings = []
    total_properties = 0

    while True:
        listings = fetch_listings_from_page(base_url, page_number, headers)

        if not listings:
            retry_count += 1
            if retry_count >= 3:
                print(f"No listings found after 3 retries on page {page_number}. Stopping.")
                break
            else:
                print(f"No listings found on page {page_number}. Retrying ({retry_count}/3)...")
                time.sleep(2)
                continue

        retry_count = 0
        all_listings.extend(listings)
        total_properties += len(listings)

        print(f"Page {page_number} scraped: {len(listings)} listings found.")
        print(f"Total properties scraped so far: {total_properties}")

        page_number += 1
        time.sleep(1)

    if all_listings:
        # Normalize the JSON data into a Pandas DataFrame
        df = pd.json_normalize(all_listings)

        # Filter out rows where property.id is null
        if 'property.id' in df.columns:
            df = df[df['property.id'].notnull()]
            print(f"Filtered listings: {len(df)} (after removing rows with null 'property.id')")

        # Save the DataFrame as a CSV file
        csv_file_path = "Output/PropertyFinder/property_listings-rent-300plus.csv"
        df.to_csv(csv_file_path, index=False)
        print(f"Data saved to {csv_file_path}")
    else:
        print("No data scraped.")


if __name__ == "__main__":
    # Define the base URL for scraping
    base_url = "https://www.propertyfinder.ae/en/search?c=4&fu=0&rp=y&ob=mr"
    scrape_properties(base_url)
