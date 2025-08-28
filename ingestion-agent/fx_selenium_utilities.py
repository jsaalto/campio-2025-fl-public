import os
import markdownify
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv
import time

def create_selenium_client():
# Setup headless Chrome
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Remove this line if you want to see the browser
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-background-networking')
    chrome_options.add_argument('--disable-background-timer-throttling')
    chrome_options.add_argument('--disable-backgrounding-occluded-windows')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-sync')
    chrome_options.add_argument('--disable-default-apps')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-gpu-sandbox')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--use-gl=swiftshader')  # Enforce software rendering
    chrome_options.add_argument('--disable-vulkan')  # Disable Vulkan backend
    chrome_options.add_argument('--disable-features=VizDisplayCompositor')
    chrome_options.add_argument('--disable-machine-learning-service')

    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"Failed to create Chrome driver: {e}")
        return None

def clean_url_for_web_call(url: str):
    url = url.strip().strip('"').strip("'").strip("[").strip("]").strip("{").strip("}").strip(",")
    parsed_url = urlparse(url)
    if not parsed_url.scheme:
        url = "https://" + url
    elif parsed_url.scheme not in ["http", "https"]:
        raise ValueError(f"Invalid URL scheme: {parsed_url.scheme}")
    return url

def capture_page_without_pwd(url):
    driver = create_selenium_client()
    try:
        driver.get(url)
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
            pass  # Ignore errors if no popups found
    
        # Wait for page to fully load and scroll to bottom
        # Scroll down progressively to ensure all content loads
        last_height = driver.execute_script("return document.body.scrollHeight")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        try:
            # Scroll back to top
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            # Get HTML
            html = driver.page_source

            # Convert HTML to Markdown
            md = markdownify.markdownify(html, heading_style="ATX")

            # Screenshot full page
            S = lambda X: driver.execute_script('return document.body.parentNode.scroll'+X)
            driver.set_window_size(S('Width'), S('Height'))
            screenshot = driver.get_screenshot_as_png()

            # Save HTML to blob storage
            html_blob_name = f"scraped_html_{int(time.time())}.html"
            blb_upload_file_to_blob(
                file_path_or_stream=html.encode('utf-8'),
                container_name="scraped-content",
                blob_name=html_blob_name,
                overwrite=True
            )
            
            # Save Markdown to blob storage
            md_blob_name = f"scraped_markdown_{int(time.time())}.md"
            blb_upload_file_to_blob(
                file_path_or_stream=md.encode('utf-8'),
                container_name="scraped-content",
                blob_name=md_blob_name,
                overwrite=True
            )
            
            # Save screenshot to blob storage
            screenshot_blob_name = f"scraped_screenshot_{int(time.time())}.png"
            blb_upload_file_to_blob(
                file_path_or_stream=screenshot,
                container_name="scraped-content",
                blob_name=screenshot_blob_name,
                overwrite=True
            )
            
            driver.quit()
            
            return {
                "html_blob": html_blob_name,
                "markdown_blob": md_blob_name,
                "screenshot_blob": screenshot_blob_name
            }
        except Exception as e:
            print(f"Error in cleanup/saving: {e}")
            driver.quit()
            return None
    
    return {
        "html_blob": html_blob_name,
        "markdown_blob": md_blob_name,
        "screenshot_blob": screenshot_blob_name
    }


def capture_page_with_pwd(url, username, pwd):
    driver = create_selenium_client()
    try:
        driver.get(url)
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
            pass  # Ignore errors if no popups found
    
        # Wait for page to fully load and scroll to bottom
        # Scroll down progressively to ensure all content loads
        last_height = driver.execute_script("return document.body.scrollHeight")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        try:
            # Scroll back to top
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            # Get HTML
            html = driver.page_source

            # Convert HTML to Markdown
            md = markdownify.markdownify(html, heading_style="ATX")

            # Screenshot full page
            S = lambda X: driver.execute_script('return document.body.parentNode.scroll'+X)
            driver.set_window_size(S('Width'), S('Height'))
            screenshot = driver.get_screenshot_as_png()

            # Save HTML to blob storage
            html_blob_name = f"scraped_html_{int(time.time())}.html"
            blb_upload_file_to_blob(
                file_path_or_stream=html.encode('utf-8'),
                container_name="scraped-content",
                blob_name=html_blob_name,
                overwrite=True
            )
            
            # Save Markdown to blob storage
            md_blob_name = f"scraped_markdown_{int(time.time())}.md"
            blb_upload_file_to_blob(
                file_path_or_stream=md.encode('utf-8'),
                container_name="scraped-content",
                blob_name=md_blob_name,
                overwrite=True
            )
            
            # Save screenshot to blob storage
            screenshot_blob_name = f"scraped_screenshot_{int(time.time())}.png"
            blb_upload_file_to_blob(
                file_path_or_stream=screenshot,
                container_name="scraped-content",
                blob_name=screenshot_blob_name,
                overwrite=True
            )
            
            driver.quit()
            
            return {
                "html_blob": html_blob_name,
                "markdown_blob": md_blob_name,
                "screenshot_blob": screenshot_blob_name
            }
        except Exception as e:
            print(f"Error in cleanup/saving: {e}")
            driver.quit()
            return None
    
    return {
        "html_blob": html_blob_name,
        "markdown_blob": md_blob_name,
        "screenshot_blob": screenshot_blob_name
    }


if __name__ == "__main__":
    driver = None
    try:
        driver = login_to_untappd()
        usrinfo = get_untappd_user_info(driver, "jeffrey_aalto_3152")
        # Use the driver for your scraping tasks
    finally:
        if driver is not None:
            driver.quit()