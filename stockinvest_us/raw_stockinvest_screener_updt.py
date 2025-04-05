import os
import json
import time
import urllib3
import logging
import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from concurrent.futures import ThreadPoolExecutor, as_completed
from tradeovant.imports.common_utils import LocalS3WithDirectory

def fetch_cookies_with_selenium(url, driver):
    driver.get(url)
    # Wait for XSRF-TOKEN to appear
    WebDriverWait(driver, 30).until(
        lambda d: d.get_cookie("XSRF-TOKEN") is not None
    )
    selenium_cookies = driver.get_cookies()
    cookies1 = {cookie["name"]: cookie["value"] for cookie in selenium_cookies}
    cookies = {
        "filter-exchanges": "NASDAQ,NYSE",
        "XSRF-TOKEN": cookies1.get("XSRF-TOKEN", "").replace("%3D", "="),
        "st_s": cookies1.get("st_s", "").replace("%3D", "="),
    }
    if not cookies["XSRF-TOKEN"] or not cookies["st_s"]:
        print("Critical cookies are missing. Ensure the page loads completely.")
        driver.quit()
        exit()
    return cookies

def send_post_request(session, url, page, headers, payload, cookies, retries=3):
    url = url + str(page)
    for attempt in range(retries):
        response = session.post(url, headers=headers, json=payload, cookies=cookies, verify=False)
        if response.status_code == 200:
            return response
        print(f"POST attempt {attempt + 1} failed for page {page}. Retrying...")
    return None

def get_trending_data(url):
    header = {
        "Host": "stockinvest.us",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    response = requests.get(url, headers=header)
    return response

if __name__ == "__main__":
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Selenium setup
    options = Options()
    options.add_argument("--ignore-certificate-errors")
    driver = webdriver.Firefox(options=options)

    # URLs
    request_url = "https://stockinvest.us/"
    post_url = "https://stockinvest.us/api/v1/screener/list?page="
    trending_url = "https://stockinvest.us/api/v1/tickers/trending/5000"

    # Headers
    headers = {
        "Host": "stockinvest.us",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": request_url,
        "Connection": "keep-alive",
    }

    # Payload
    payload = {
        "fields": [],
        "query": {
            "condition": "AND",
            "rules": [
                {"id": "exchange_id", "field": "exchange_id", "operator": "in", "value": [1, 4, 6, 7, 27]}
            ]
        },
        "sort": {"field": "symbol", "direction": "ASC"},
    }

    file_version_date = time.strftime("%Y%m%d")
    storage = LocalS3WithDirectory()
    logging.info("Initiated Remote Storage")
    screener_file = f"screener_data_{file_version_date}.json"
    screener_path = os.path.join(storage.BASE_PATH, "raw_store", "stockinvest_us", "screener_data")
    trending_file = f"trending_data_{file_version_date}.json"
    trending_path = os.path.join(storage.BASE_PATH, "raw_store", "stockinvest_us", "trending_data")

    # Make sure the directory exists
    os.makedirs(screener_path, exist_ok=True)
    os.makedirs(trending_path, exist_ok=True)

    try:
        # Fetch cookies dynamically using Selenium
        cookies = fetch_cookies_with_selenium(request_url, driver)
        print("Fetched Cookies:", cookies)
        driver.quit()  # Close the browser immediately after fetching cookies

        # Initialize session
        session = requests.Session()
        session.get(request_url, headers=headers, cookies=cookies, verify=False)
        headers["x-xsrf-token"] = cookies["XSRF-TOKEN"]

        # Fetch page 1 to determine the total number of pages
        post_response = send_post_request(session, post_url, 1, headers, payload, cookies)
        if not post_response:
            print("Failed to fetch page 1")
            exit()

        response_json = post_response.json()
        data_page1 = response_json.get("data", [])
        last_page = response_json.get("last_page", 1)
        print(f"Total pages: {last_page}")

        # Initialize storage for page data
        page_data = {1: data_page1}

        # Fetch remaining pages (2 to last_page) concurrently
        pages_to_fetch = list(range(2, last_page + 1))
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit tasks for each page
            future_to_page = {
                executor.submit(send_post_request, session, post_url, page, headers, payload, cookies): page
                for page in pages_to_fetch
            }
            # Collect results as they complete
            for future in as_completed(future_to_page):
                page = future_to_page[future]
                try:
                    post_response = future.result()
                    if post_response:
                        response_json = post_response.json()
                        data = response_json.get("data", [])
                        page_data[page] = data
                        print(f"Processed page {page}")
                    else:
                        print(f"Failed to fetch page {page}")
                except Exception as e:
                    print(f"Error fetching page {page}: {e}")

        # Combine all data in page order
        all_data = []
        for page in range(1, last_page + 1):
            if page in page_data:
                all_data.extend(page_data[page])

        # Save combined screener data
        if all_data:
            output_path = os.path.join(screener_path, screener_file)
            with open(output_path, "w") as file:
                json.dump(all_data, file, indent=4)
            print(f"Screener Data successfully saved to: {output_path}")
        else:
            print("No screener data fetched.")

        # Fetch trending data (unchanged)
        trending_response = get_trending_data(trending_url)
        if trending_response:
            output_path = os.path.join(trending_path, trending_file)
            with open(output_path, "w") as file:
                json.dump(trending_response.json(), file, indent=4)
            print(f"Trending Data successfully saved to: {output_path}")
        else:
            print("Trending Data request failed")

    finally:
        storage.stop()