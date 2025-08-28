from fx_db import db_get_homepage_process_list
from fx_homepage_processing import homepage_processing_main


def process_homepage(url: str):
    hp_facebook_response = homepage_processing_main(url)
    print(f"Processing homepage: {url}")

def process_facebook_page(url: str):
    print(f"Processing Facebook page: {url}")

def process_instagram_page(url: str):
    print(f"Processing Instagram page: {url}")

def process_twitter_page(url: str):
    print(f"Processing Twitter page: {url}")

def process_gmail_messages():
    print("Processing Gmail messages")

if __name__ == "__main__":
    process_list = db_get_homepage_process_list()
    print(process_list)
    for url in process_list:
        submit_url = url[0] if isinstance(url, list) else url
        print(submit_url)
        hp_response = homepage_processing_main(submit_url)
