import os
import time
import markdownify
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import sys

def create_selenium_client():
# Setup headless Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--allow-running-insecure-content")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-field-trial-config")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--silent")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"Failed to create Chrome driver: {e}")
        return None

        def login_to_site(driver, base_url, username, password):
            """
            Attempts to log into a website using common login patterns.
            Returns True if login appears successful, False otherwise.
            """
            try:
                driver.get(base_url)
                time.sleep(2)
                
                # Common login link/button selectors
                login_selectors = [
                    'a[href*="login"]',
                    'a[href*="signin"]',
                    'button[class*="login"]',
                    'button[class*="signin"]',
                    '[data-testid*="login"]',
                    '[id*="login"]',
                    'a:contains("Login")',
                    'a:contains("Sign In")'
                ]
                
                # Try to find and click login link
                login_clicked = False
                for selector in login_selectors:
                    try:
                        elements = driver.find_elements("css selector", selector)
                        for element in elements:
                            if element.is_displayed():
                                element.click()
                                login_clicked = True
                                time.sleep(2)
                                break
                        if login_clicked:
                            break
                    except:
                        continue
                
                # Common username field selectors
                username_selectors = [
                    'input[name="username"]',
                    'input[name="email"]',
                    'input[type="email"]',
                    'input[id*="username"]',
                    'input[id*="email"]',
                    'input[placeholder*="username"]',
                    'input[placeholder*="email"]'
                ]
                
                # Common password field selectors
                password_selectors = [
                    'input[name="password"]',
                    'input[type="password"]',
                    'input[id*="password"]'
                ]
                
                # Fill username
                username_filled = False
                for selector in username_selectors:
                    try:
                        username_field = driver.find_element("css selector", selector)
                        if username_field.is_displayed():
                            username_field.clear()
                            username_field.send_keys(username)
                            username_filled = True
                            break
                    except:
                        continue
                
                # Fill password
                password_filled = False
                for selector in password_selectors:
                    try:
                        password_field = driver.find_element("css selector", selector)
                        if password_field.is_displayed():
                            password_field.clear()
                            password_field.send_keys(password)
                            password_filled = True
                            break
                    except:
                        continue
                
                if not (username_filled and password_filled):
                    return False
                
                # Submit form - try different submit methods
                submit_selectors = [
                    'button[type="submit"]',
                    'input[type="submit"]',
                    'button[class*="login"]',
                    'button[class*="signin"]',
                    'button:contains("Login")',
                    'button:contains("Sign In")'
                ]
                
                submitted = False
                for selector in submit_selectors:
                    try:
                        submit_button = driver.find_element("css selector", selector)
                        if submit_button.is_displayed():
                            submit_button.click()
                            submitted = True
                            break
                    except:
                        continue
                
                if not submitted:
                    # Try submitting the password field directly
                    try:
                        password_field.send_keys("\n")
                    except:
                        return False
                
                time.sleep(3)  # Wait for login to process
                
                # Check if login was successful by looking for common indicators
                current_url = driver.current_url
                page_source = driver.page_source.lower()
                
                # Success indicators
                if ("dashboard" in current_url or "profile" in current_url or 
                    "account" in current_url or "welcome" in page_source or
                    "logout" in page_source):
                    return True
                
                # Failure indicators
                if ("error" in page_source or "invalid" in page_source or
                    "incorrect" in page_source or "login" in current_url):
                    return False
                
                return True  # Assume success if no clear indicators
                
            except Exception as e:
                print(f"Login attempt failed: {e}")
                return False

def capture_page(url, username, pwd):
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
            # blb_upload_file_to_blob(
            #     file_path_or_stream=html.encode('utf-8'),
            #     container_name="scraped-content",
            #     blob_name=html_blob_name,
            #     overwrite=True
            # )
            
            # Save Markdown to blob storage
            md_blob_name = f"scraped_markdown_{int(time.time())}.md"
            # blb_upload_file_to_blob(
            #     file_path_or_stream=md.encode('utf-8'),
            #     container_name="scraped-content",
            #     blob_name=md_blob_name,
            #     overwrite=True
            # )
            
            # Save screenshot to blob storage
            screenshot_blob_name = f"scraped_screenshot_{int(time.time())}.png"
            # blb_upload_file_to_blob(
            #     file_path_or_stream=screenshot,
            #     container_name="scraped-content",
            #     blob_name=screenshot_blob_name,
            #     overwrite=True
            # )
            
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

# if __name__ == "__main__":
#     url = "https://www.couchdogbrewing.com/beer"
#     capture_page(url)