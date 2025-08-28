import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv
import sys

def login_to_untappd():
    # Get credentials from environment variables
    load_dotenv()
    username = os.getenv('UNTAPPD_USERNAME')
    password = os.getenv('UNTAPPD_PASSWORD')
    
    if not username or not password:
        raise ValueError("UNTAPPD_USERNAME and UNTAPPD_PASSWORD environment variables must be set")
        
    # Create driver instance
    driver = create_selenium_client()
    if driver is None:
        raise Exception("Failed to create Selenium driver")

    try:
        # Navigate to Untappd login page
        driver.get("https://untappd.com/login")
        
        try:
            # Wait for and fill username field
            username_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "username"))
            )
            username_field.send_keys(username)
            
            # Fill password field
            password_field = driver.find_element(By.NAME, "password")
            password_field.send_keys(password)
            
            # Submit the form
            try:
                # Try multiple selectors for the login button
                login_selectors = [
                    "//input[@type='submit' and contains(@value, 'Sign In')]",
                    "//button[contains(text(), 'Sign In')]",
                    "//span[contains(@class, 'submit-btn') and contains(text(), 'Sign In')]",
                    "//span[contains(@class, 'button') and contains(text(), 'Sign In')]",
                    "//input[@type='submit']",
                    "//button[@type='submit']",
                    "//form//input[@type='submit']"
                    ]
                
                login_button = None
                for selector in login_selectors:
                    try:
                        login_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                        )
                        break
                    except:
                        continue
                
                if login_button is None:
                    raise Exception("Could not find login button")
                
                # Check if there's a captcha present before clicking login
                captcha_present = False
                captcha_selectors = [
                    "//iframe[contains(@src, 'recaptcha')]",
                    "//*[contains(@class, 'captcha')]",
                    "//*[contains(@id, 'captcha')]",
                    "//div[contains(@class, 'g-recaptcha')]"
                    ]
                
                for selector in captcha_selectors:
                    try:
                        captcha_element = driver.find_element(By.XPATH, selector)
                        if captcha_element.is_displayed():
                            captcha_present = True
                            break
                    except:
                        continue
                
                if captcha_present:
                    # Wait for user to solve captcha and login to complete
                    print("Captcha detected. Waiting for login to complete...")
                    try:
                        # Wait for URL to change (indicating successful login)
                        WebDriverWait(driver, 60).until(
                            lambda d: "login" not in d.current_url.lower()
                        )
                        print("Login completed after captcha")
                    except:
                        # Alternative: wait for presence of elements that appear after login
                        try:
                            WebDriverWait(driver, 60).until(
                                EC.any_of(
                                    EC.presence_of_element_located((By.CLASS_NAME, "user-menu")),
                                    EC.presence_of_element_located((By.CLASS_NAME, "profile-menu")),
                                    EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/user/')]"))
                                )
                            )
                            print("Login completed after captcha (detected user elements)")
                        except:
                            raise Exception("Captcha not completed within timeout period")
                else:
                    # No captcha, proceed with normal login
                    login_button.click()
                    
                    # Wait for login to complete
                    WebDriverWait(driver, 10).until(
                        lambda driver: driver.execute_script("return document.readyState") == "complete"
                    )
            
            except Exception as e:
                raise Exception(f"Failed to handle login submission: {str(e)}")
        except:
            pass

        print("Successfully logged into Untappd")
        return driver
        
    except Exception as e:
        driver.quit()
        raise Exception(f"Login failed: {str(e)}")

def get_untappd_user_info(driver, profilename):
    # Navigate to the user's profile page
    driver.get(f"https://untappd.com/user/{profilename}/beers")

    try:
        # Wait for the page to load - try multiple selectors as fallback
        try:
            WebDriverWait(driver, 30).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CLASS_NAME, "content")),
                    EC.presence_of_element_located((By.CLASS_NAME, "main-content")),
                    EC.presence_of_element_located((By.TAG_NAME, "main")),
                    EC.presence_of_element_located((By.CLASS_NAME, "user-content"))
                )
            )
        except:
            pass

        # Continuous cycle of scrolling and clicking "Show More" until all content is loaded
        show_more_selectors = [
            "//button[contains(text(), 'Show More')]",
            "//a[contains(text(), 'Show More')]",
            "//button[contains(@class, 'show-more')]",
            "//a[contains(@class, 'show-more')]",
            "//button[contains(text(), 'Load More')]",
            "//a[contains(text(), 'Load More')]"
        ]
        
        while True:
            # First, scroll to the bottom of the page
            last_height = driver.execute_script("return document.body.scrollHeight")
            scroll_attempts = 0
            max_scroll_attempts = 10
            
            while scroll_attempts < max_scroll_attempts:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)  # Wait for content to load
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    scroll_attempts += 1
                else:
                    scroll_attempts = 0
                    last_height = new_height
            
            # After scrolling, look for and click "Show More" button
            show_more_clicked = False
            for selector in show_more_selectors:
                try:
                    show_more_button = driver.find_element(By.XPATH, selector)
                    if show_more_button.is_displayed() and show_more_button.is_enabled():
                        driver.execute_script("arguments[0].scrollIntoView();", show_more_button)
                        time.sleep(2)
                        show_more_button.click()
                        print("Clicked 'Show More' button")
                        time.sleep(5)  # Wait for new content to load after button click
                        show_more_clicked = True
                    break
                except:
                    continue
            
            # If no "Show More" button was found or clicked, we're done
            if not show_more_clicked:
                print("No more 'Show More' buttons found - finished loading all content")
                break

        print(f"Successfully loaded all content for user {profilename}")

        # Save the page source as HTML file
        html_content = driver.page_source
        filename = f"untappd_user_{profilename}.html"

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"Page saved as {filename}")
        return filename

    except Exception as e:
        print(f"Error loading user info for profile '{profilename}': {str(e)}")
        raise

# if __name__ == "__main__":
#     url = "https://www.couchdogbrewing.com/beer"
#     capture_page(url)