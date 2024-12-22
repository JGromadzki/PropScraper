import requests
import time
import json
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any
import logging
import os
import glob

class MagnoliaScraper:
    def __init__(self, app_id: str, api_key: str, index_name: str):
        """Initialize the Magnolia scraper with Algolia credentials."""
        self.app_id = app_id
        self.api_key = api_key
        self.index_name = index_name
        self.base_url = f"https://{app_id}-dsn.algolia.net/1/indexes/{index_name}/query"
        self.headers = {
            'X-Algolia-API-Key': api_key,
            'X-Algolia-Application-Id': app_id,
            'Content-Type': 'application/json'
        }

        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # Create output directory if it doesn't exist
        os.makedirs('output/Bayut', exist_ok=True)

    def _make_request(self, payload: Dict) -> Dict:
        """Make a request to the Algolia API with rate limiting and error handling."""
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    self.base_url,
                    headers=self.headers,
                    json=payload
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    raise

    def scrape_listings(self, 
                        query: str = "", 
                        filters: str = "", 
                        page: int = 0, 
                        hits_per_page: int = 50) -> List[Dict]:
        """
        Scrape property listings with given parameters.
        
        Args:
            query: Search query string
            filters: Algolia filters string
            page: Page number to fetch
            hits_per_page: Number of results per page
        
        Returns:
            List of property listings
        """
        payload = {
            "query": query,
            "filters": filters,
            "page": page,
            "hitsPerPage": hits_per_page
        }

        try:
            result = self._make_request(payload)
            return result.get('hits', [])
        except Exception as e:
            self.logger.error(f"Failed to scrape listings: {str(e)}")
            return []

    def price_range_scrape(self, 
                            base_filters: str = "purpose:for-rent", 
                            max_pages: int = 1, 
                            hits_per_page: int = 50):
        """
        Scrape listings by dividing into price ranges.
        
        Args:
            base_filters: Base filters to apply to all queries
            max_pages: Maximum pages to scrape per price range
            hits_per_page: Number of hits per page
        """
        # Define price ranges with descriptive labels
        price_ranges = [
            {"label": "budget", "filter": "price < 20000"},
           {"label": "budget2", "filter": "price >= 20001 AND price < 35000"},
           {"label": "budget3", "filter": "price >= 35001 AND price < 50000"},
            {"label": "mid_range", "filter": "price >= 50001 AND price < 75000"},
            {"label": "mid_range2", "filter": "price >= 75001 AND price < 100000"},
           {"label": "mid_range22", "filter": "price >= 100001 AND price < 120000"},
           {"label": "mid_range3", "filter": "price >= 120001 AND price < 150001"},
            {"label": "mid_range4", "filter": "price >= 150001 AND price < 200000"},
            {"label": "mid_range5", "filter": "price >= 200001 AND price < 250000"},
            {"label": "mid_range6", "filter": "price >= 250001 AND price < 275000"},
            {"label": "mid_range7", "filter": "price >= 275001 AND price < 300000"},
            {"label": "mid_range8", "filter": "price >= 300001 AND price < 400000"},
            {"label": "mid_range9", "filter": "price >= 400001 AND price < 500000"},
            {"label": "mid_range10", "filter": "price >= 500001 AND price < 600000"},
            ##{"label": "mid_range5", "filter": "price >= 200001 AND price < 300000"},
            #{"label": "luxury", "filter": "price >=  5000000 AND price < 6000000"},
            #{"label": "luxury2", "filter": "price >= 6000000 AND price < 8000000"},
            #{"label": "luxury3", "filter": "price >= 8000000 AND price < 10000000"},
            #{"label": "luxury4", "filter": "price >= 10000000 AND price < 12000000"},
            #{"label": "luxury5", "filter": "price >= 12000000 AND price < 15000000"},
            {"label": "ultra_luxury", "filter": "price >= 600001"}
            #{"label": "test", "filter": "price >= 0"}
        ]

        # Iterate through price ranges
        for price_range in price_ranges:
            try:
                # Combine base filters with price range filter
                combined_filter = f"{base_filters} AND {price_range['filter']}"
                
                # Output file path
                output_file = os.path.join(
                    'output/Bayut', 
                    #'output/', 
                    f"bayut_listings_{price_range['label']}_pages_{max_pages}.csv"
                )

                # Scrape listings for this price range
                all_listings = []

                self.logger.info(f"Scraping {price_range['label']} price range")

                for page in range(max_pages):
                    self.logger.info(f"Scraping page {page + 1}/{max_pages} for {price_range['label']} range")
                    
                    listings = self.scrape_listings(
                        filters=combined_filter,
                        page=page,
                        hits_per_page=hits_per_page
                    )

                    if not listings:
                        self.logger.info(f"No more listings found for {price_range['label']} range at page {page + 1}")
                        break

                    all_listings.extend(listings)
                    time.sleep(1)  # Rate limiting

                # Save to CSV if listings found
                if all_listings:
                    df = pd.DataFrame(all_listings)
                    df['scrape_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # Process the 'geography' column to extract latitude and longitude
                    df['latitude'] = df['geography'].apply(lambda x: x['lat'] if isinstance(x, dict) and 'lat' in x else None)
                    df['longitude'] = df['geography'].apply(lambda x: x['lng'] if isinstance(x, dict) and 'lng' in x else None)
        
                # Process the 'location' column to extract City, District, and Neighborhood details
                    def extract_location_details(location, level):
                        if isinstance(location, list):
                            for loc in location:
                                if loc.get('level') == level:
                                    return loc.get('externalID'), loc.get('name')
                        return None, None
        
                    # Extract City details (level 1)
                    df['CityCode'], df['CityName'] = zip(*df['location'].apply(lambda loc: extract_location_details(loc, level=1)))
        
                    # Extract District details (level 2)
                    df['DistrictID'], df['DistrictName'] = zip(*df['location'].apply(lambda loc: extract_location_details(loc, level=2)))
        
                    # Extract Neighborhood details (level 3)
                    df['NeighborhoodID'], df['NeighborhoodName'] = zip(*df['location'].apply(lambda loc: extract_location_details(loc, level=3)))

                    # Extract Neighborhood details (level 3)
                    df['BuildingID'], df['BuildingName'] = zip(*df['location'].apply(lambda loc: extract_location_details(loc, level=4)))
        


                    # Process the 'category' column to extract Category Type and Subtype
                    def extract_category_details(category, level):
                        if isinstance(category, list):
                            for cat in category:
                                if cat.get('level') == level:
                                    return cat.get('externalID'), cat.get('name')
                        return None, None

                    # Extract Category Type (level 0)
                    df['CategoryTypeCode'], df['CategoryTypeName'] = zip(*df['category'].apply(lambda cat: extract_category_details(cat, level=0)))
        
        
                    df['CategorySubtypeCode'], df['CategorySubtypeName'] = zip(*df['category'].apply(lambda cat: extract_category_details(cat, level=1)))


                    # Extract agency name from 'agency' column
                    df['AgencyName'] = df['agency'].apply(lambda x: x.get('name') if isinstance(x, dict) and 'name' in x else None)
        
        # Extract 'dldBuildingNK' and 'dldPropertySK' from 'extraFields' column
                    df['DldBuildingNK'] = df['extraFields'].apply(lambda x: x.get('dldBuildingNK') if isinstance(x, dict) and 'dldBuildingNK' in x else None)
                    df['DldPropertySK'] = df['extraFields'].apply(lambda x: x.get('dldPropertySK') if isinstance(x, dict) and 'dldPropertySK' in x else None)




                    # Convert 'createdAt' column to timestamp
                    df['createdAt'] = pd.to_datetime(df['createdAt'], unit='s')
                    df['updatedAt'] = pd.to_datetime(df['updatedAt'], unit='s')

                    # Create 'BayutLink' column using 'externalID'
                    df['BayutLink'] = df['externalID'].apply(lambda x: f"https://www.bayut.com/property/details-{x}.html" if pd.notnull(x) else None)


                    # Rename column 'area' to 'area (sqm)'
                    df.rename(columns={'area': 'area (sqm)'}, inplace=True)
                    df.rename(columns={'plotArea': 'plotArea (sqm)'}, inplace=True)

                    # Define the desired columns and their order
                    desired_columns = [
                        'objectID', 'DldPropertySK', 'referenceNumber', 'permitNumber', 'title', 'BayutLink', 'purpose', 'price', 'rentFrequency', 'rooms',
                         'baths', 'area (sqm)', 'plotArea (sqm)', 'furnishingStatus',  'amenities',
                        'CategoryTypeName', 'CategorySubtypeName', 
                        'CityName', 'DistrictID', 'DistrictName','NeighborhoodName', 'BuildingName', 'DldBuildingNK', 'latitude', 'longitude', 'createdAt', 
                        'updatedAt',
                        'contactName', 'phoneNumber','AgencyName', 
                        'completionStatus', 'scrape_date']

                    # Retain only the desired columns in the specified order
                    df = df[desired_columns]

                    df = df[df['CityName'] == 'Dubai']

                    df.to_csv(output_file, index=False)
                    self.logger.info(f"Successfully saved {len(all_listings)} listings to {output_file}")
                else:
                    self.logger.warning(f"No listings found for {price_range['label']} price range")

            except Exception as e:
                self.logger.error(f"Error scraping {price_range['label']} price range: {str(e)}")



    @staticmethod
    def combine_csv_files(input_folder: str, output_file: str):
        """
        Combine all CSV files from the input folder into a single CSV file.
        
        Args:
            input_folder: Path to the folder containing CSV files to combine.
            output_file: Path to the consolidated output file.
        """
        try:
            # Get list of all CSV files in the folder
            csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
            if not csv_files:
                logging.warning(f"No CSV files found in folder: {input_folder}")
                return
            
            logging.info(f"Found {len(csv_files)} files to combine.")

            # Read and concatenate all files
            combined_df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)
            
            # Save the combined DataFrame to the output file
            combined_df.to_csv(output_file, index=False)
            logging.info(f"Successfully combined files into {output_file}")
        except Exception as e:
            logging.error(f"Failed to combine CSV files: {str(e)}")


def main():
    # Initialize scraper with Algolia credentials
    scraper = MagnoliaScraper(
        app_id="LL8IZ711CS",
        api_key="15cb8b0a2d2d435c6613111d860ecfc5",
        index_name="bayut-production-ads-city-level-score-en"
    )
    
    # Scrape listings with price range pagination
    scraper.price_range_scrape(
        base_filters="purpose:for-rent",
        max_pages=1000,  # Adjust as needed
        hits_per_page=50
    )

    # Combine all CSV files in the output folder
    output_folder = "output/Bayut"
    #output_folder = "output/"

    # Generate the file name with the current date
    scraping_date = datetime.now().strftime('%Y-%m-%d')
    output_file = os.path.join(output_folder, f"Bayut-all_listings-rent_{scraping_date}.csv")

    scraper.combine_csv_files(input_folder=output_folder, output_file=output_file)

if __name__ == "__main__":
    main()