from fx_selenium_utilities import create_selenium_client, clean_url_for_web_call
from fx_generic_agents import invoke_llm_agent
from fx_db import db_facebook_business_page_upsert, db_instagram_page_upsert, db_x_page_upsert, db_bluesky_page_upsert, db_mastodon_page_upsert, db_venue_upsert, db_logo_url_upsert
from fx_db import db_get_establishment_name, db_get_venue_name, db_establishment_upsert
from fx_cu import cu_analyzer_main
from fx_blb import blb_upload_file_to_blob
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import markdownify
import time
import json

def homepage_processing_main(url: str):
    if hasattr(url, 'url'):  # Check if 'url' has an attribute 'url'
        url = url.url  # Extract the string value from the Row object
    if not isinstance(url, str):
        raise ValueError(f"Expected a string for 'url', but got {type(url).__name__}")
    clean_url = clean_url_for_web_call(url)
    print(f"Processing homepage: {clean_url}")
    driver = create_selenium_client()
    if driver is None:
        print("Failed to initialize Selenium client.")
    try:
        driver.get(clean_url)
        time.sleep(2)  # Wait for page to load
            
        # Clear any popups/modals
        try:
            # Common popup selectors
            popup_selectors = [
                '[class*="popup"]',
                '[data-dismiss="modal"]'
            ]
            
            for selector in popup_selectors:
                elements = driver.find_elements("css selector", selector)
                for element in elements:
                    if element.is_displayed():
                        element.click()
                        time.sleep(0.5)
                        break
        except:
            pass
        try: 
            analyzer_name =  "cu-business-type-from-webpage-analyzer"
            html = driver.page_source
            html_blob_name = f"scraped_html_{int(time.time())}.html"
            blob_url = blb_upload_file_to_blob(
                file_path_or_stream=html.encode('utf-8'),
                container_name="scraped-content",
                blob_name=html_blob_name,
                overwrite=True
            )
            print(f"HTML uploaded to blob: {blob_url}")
            cu_response = cu_analyzer_main(blob_url, analyzer_name)
            print(f"CU Response: {cu_response}")
        except Exception as e:
            print(f"Error retrieving page source: {e}")
        try: 
            # Establishment if it's not already there
            establishment = clean_url.replace("http://", "").replace("https://", "").replace("www.", "").split("/")[0] + '#web'
            resp = db_get_establishment_name(establishment)
            if resp:
                print(f"Establishment found: {resp}")
            else:
                establishment_data = {
                    "establishment": establishment,
                    "establishment_name": cu_response.get("establishment_name"),
                    "business_type": cu_response.get("business_type")
                }
                estb_response = db_establishment_upsert(establishment_data)
                print(f"Establishment created: {estb_response}")
        except Exception as e:
            print(f"Error retrieving establishment name: {e}")        
        try:
            # Find the business logo image
            # Try to find logo with priority order - start with exact "logo" matches first
            logo_selectors = [
                "//img[contains(@class, 'logo') or contains(@alt, 'logo') or contains(@id, 'logo') or @itemprop='logo']",
                "//img[contains(@class, 'site-logo') or contains(@class, 'header-logo') or contains(@class, 'nav-logo') or contains(@class, 'company-logo')]",
                "//img[contains(@class, 'brand') or contains(@alt, 'brand') or contains(@id, 'brand')]",
                "//img[contains(@alt, 'company') or contains(@class, 'identity') or contains(@alt, 'identity')]"
            ]
            
            logo_element = None
            for selector in logo_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    if elements:
                        logo_element = elements[0]  # Take the first element found
                        break
                except:
                    continue
            
            if logo_element is None:
                raise NoSuchElementException("No logo element found")
            logo_url = logo_element.get_attribute('src')
            print(f"Logo found: {logo_url}")
            db_logo_url_upsert(logo_url, clean_url)
        except NoSuchElementException:
            print("No Logo link found on the homepage.")
        try:
            # Find the link to the company's Facebook page
            facebook_link = driver.find_element(By.XPATH, "//a[contains(@href, 'facebook.com')]")
            facebook_url = facebook_link.get_attribute('href')
            if facebook_url.endswith('/'):
                facebook_url = facebook_url.rstrip('/')
            print(f"Facebook link found: {facebook_url}")
            db_facebook_business_page_upsert(facebook_url, clean_url)
        except NoSuchElementException:
            print("No Facebook link found on the homepage.")
        try:
            # Find the link to the company's Instagram page
            instagram_link = driver.find_element(By.XPATH, "//a[contains(@href, 'instagram.com')]")
            instagram_url = instagram_link.get_attribute('href')
            if instagram_url.endswith('/'):
                instagram_url = instagram_url.rstrip('/')
            print(f"Instagram link found: {instagram_url}")
            # You may need to create a similar function for Instagram or modify the existing one
            db_instagram_page_upsert(instagram_url, clean_url)
        except NoSuchElementException:
            print("No Instagram link found on the homepage.")
        try:
            # Find the link to the company's X (Twitter) page
            x_link = driver.find_element(By.XPATH, "//a[contains(@href, 'twitter.com') or contains(@href, 'x.com')]")
            x_url = x_link.get_attribute('href')
            if x_url.endswith('/'):
                x_url = x_url.rstrip('/')
            print(f"X (Twitter) link found: {x_url}")
            # You may need to create a similar function for X or modify the existing one
            db_x_page_upsert(x_url, clean_url)
        except NoSuchElementException:
            print("No X (Twitter) link found on the homepage.")
        try:
            # Find the link to the company's Bluesky page
            bluesky_link = driver.find_element(By.XPATH, "//a[contains(@href, 'bsky.app')]")
            bluesky_url = bluesky_link.get_attribute('href')
            if bluesky_url.endswith('/'):
                bluesky_url = bluesky_url.rstrip('/')
            print(f"Bluesky link found: {bluesky_url}")
            # You may need to create a similar function for Bluesky or modify the existing one
            db_bluesky_page_upsert(bluesky_url, clean_url)
        except NoSuchElementException:
            print("No Bluesky link found on the homepage.")
        try:
            # Find the link to the company's Mastodon page
            mastodon_link = driver.find_element(By.XPATH, "//a[contains(@href, 'mastodon')]")
            mastodon_url = mastodon_link.get_attribute('href')
            if mastodon_url.endswith('/'):
                mastodon_url = mastodon_url.rstrip('/')
            print(f"Mastodon link found: {mastodon_url}")
            # You may need to create a similar function for Mastodon or modify the existing one
            db_mastodon_page_upsert(mastodon_url, clean_url)
        except NoSuchElementException:
            print("No Mastodon link found on the homepage.")
        # Location Processing
        
        try: 
            html = driver.page_source
            # Convert HTML to Markdown
            md = markdownify.markdownify(html, heading_style="ATX")            
            location_list = invoke_llm_agent("locations", md)
            try:
                # location_list may be an object with a 'content' attribute containing a JSON string
                if hasattr(location_list, 'content'):
                    content = location_list.content
                else:
                    content = location_list
                print(f"Location List Content: {content}")
                # If content is a string, try to parse it as JSON
                if isinstance(content, str):
                    locations = json.loads(content)
                else:
                    locations = content

                # Extract 'link' values, handling "/" as homepage_url
                for loc in locations:
                    if isinstance(loc, dict) and 'link' in loc:
                        link = loc['link']
                        if link == "/":
                            venue_url = clean_url
                        elif link.startswith('/'):
                            venue_url = clean_url.rstrip('/') + link
                        else:
                            venue_url = link
                        venue_attribute_data = venue_processing(venue_url)
                        if venue_attribute_data == "404 or not found page detected":
                            # PUT PARSE OF CU RESPONSE DATA HERE                            
                            print(f"Skipping location due to 404 or not found: {venue_url}")
            except Exception as e:
                print(f"Error parsing location links: {e}")
        except Exception as e:
            print(f"An error occurred while processing locations: {e}")
            location_list = "[]"
    except Exception as e:
        print(f"An error occurred while searching for the Facebook link: {e}")
    finally:
        driver.quit()


def venue_processing(url: str):
    if hasattr(url, 'url'):  # Check if 'url' has an attribute 'url'
        url = url.url  # Extract the string value from the Row object
        print(f"Extracted URL: {url}")
    if not isinstance(url, str):
        raise ValueError(f"Expected a string for 'url', but got {type(url).__name__}")
    clean_url = clean_url_for_web_call(url)
    print(f"Processing homepage: {clean_url}")
    driver = create_selenium_client()
    if driver is None:
        print("Failed to initialize Selenium client.")
    try:
        driver.get(clean_url)
        time.sleep(2)  # Wait for page to load
            
        # Clear any popups/modals
        try:
            # Common popup selectors
            popup_selectors = [
                '[class*="popup"]',
                '[data-dismiss="modal"]'
            ]
            
            for selector in popup_selectors:
                elements = driver.find_elements("css selector", selector)
                for element in elements:
                    if element.is_displayed():
                        element.click()
                        time.sleep(0.5)
                        break
        except:
            pass
        try: 
            html = driver.page_source
            # Check if page contains 404 or "not found" in the body
            body_text = driver.find_element(By.TAG_NAME, "body").text.lower()
            if "404" in body_text and "not found" in body_text:
                print("Page contains 404 and 'not found' - exiting venue processing")
                return "404 or not found page detected"
            # Convert HTML to Markdown
            md = markdownify.markdownify(html, heading_style="ATX")          
            venue_page_response = invoke_llm_agent("venue homepage", md)
            if venue_page_response is None:
                return "failed parsing the venue page"
            if venue_page_response:
                print("Venue Page Response Received - Parsing JSON")
                venue_data = json.loads(venue_page_response.content)
                if venue_data is None:
                    print("failed parsing the venue page")
                elif venue_data:
                    print("Venue Page Parsed and converted to JSON - Database Processing Next")
                    venue_upsert_response = db_venue_upsert(venue_data)
            return "successful venue processing"
        except Exception as e:
            print(f"An error occurred while processing the venue: {e}")
            return "failed parsing the venue page"
    except Exception as e:
        print(f"An error occurred while processing the venue: {e}")
        return "failed parsing the venue page"


if __name__ == "__main__":
    # submit_url = "https://trilliumbrewing.com"
    submit_url = "https://chrisfarmstand.com"
    # submit_url = "https://barreledsouls.com"
    hp_response = homepage_processing_main(submit_url)

