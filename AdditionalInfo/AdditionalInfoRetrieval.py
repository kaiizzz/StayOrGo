import json
import time
import re
from collections import defaultdict
import requests, json, urllib.parse as up

RED="\033[91m"; GREEN="\033[92m"; YELLOW="\033[93m"; RESET="\033[0m"

REG_URL = "https://api.cdr.gov.au/cdr-register/v1/all/data-holders/brands/summary"
COMMON_HEADERS = {
    "Accept": "application/json",
    "x-v": "7",          # ok for banking products
    "x-min-v": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

class AdditionalInfoRetrieval:
    def __init__(self):
        pass

    def fetch_brands(self):
        try:
            response = requests.get(REG_URL, headers=COMMON_HEADERS)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"{RED}Failed to fetch brands: {response.status_code}{RESET}")
                return None
        except Exception as e:
            print(f"{RED}Error fetching brands: {e}{RESET}")
            return None
        
    def fetch_brand_products(self, brand_uri):
        url = f"{brand_uri}/cds-au/v1/banking/products"
        try:
            response = requests.get(url, headers=COMMON_HEADERS)
            if response.status_code == 200:
                # print(f"{GREEN}Successfully fetched products for brand {brand_uri}!{RESET}")
                return response.json()
            else:
                # print(f"{RED}Failed to fetch products for brand {brand_uri}: {response.status_code}{RESET}")
                return None
        except Exception as e:
            print(f"{RED}Error fetching products for brand {brand_uri}: {e}{RESET}")
            return None
        


def main():
    retriever = AdditionalInfoRetrieval()
    brands_data = retriever.fetch_brands()
    if brands_data:
        print(f"{GREEN}Successfully fetched brands data!{RESET}")
        # For demonstration, print the first 3 brands
        for brand in brands_data.get('data', []):
            #print(json.dumps(brand, indent=2))
            # print (brand)
            brand_uri = brand.get('publicBaseUri')
            # print(brand_uri)
            if brand_uri:
                products_data = retriever.fetch_brand_products(brand_uri)
                if products_data:
                    #print(f"{GREEN}Successfully fetched products for brand {brand_uri}!{RESET}")
                    # print(json.dumps(products_data, indent=2))
                    pass
                else:
                    print(f"{RED}No products data retrieved for brand {brand_uri}.{RESET}")
    else:
        print(f"{RED}No brands data retrieved.{RESET}")
    
    print(f"{GREEN}Showing {len(brands_data.get('data', []))} brands in total.{RESET}")

if __name__ == "__main__":    
    main()