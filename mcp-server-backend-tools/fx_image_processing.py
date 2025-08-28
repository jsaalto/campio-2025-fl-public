from typing import Optional
from fx_cu import cu_analyzer_main
from fx_db import db_upsert_wifi_network_password, db_image_to_content_upsert, db_output_list_to_stage, db_create_new_business_from_image
from fx_db import db_get_business_list_by_lat_lon, db_stage_wifi_data, db_get_venue_name, db_stage_hours_of_operation_data, db_stage_product_offering_data
from fx_blb import blb_upload_file_to_blob, blb_get_blob_size
from fx_utilities import get_gps_coordinates, get_street_address_from_lat_lon
import os
from urllib.parse import urlparse
from PIL import Image
import requests
from io import BytesIO
import base64
import tempfile
import uuid
from datetime import datetime

def img_initial_image_process_stream(image_stream: BytesIO, filename: Optional[str] = None, venue: Optional[str] = None, lat: Optional[float] = None, lon: Optional[float] = None):
    # Step 1 - Get Latitude and Longitude and classify the image
    
    venue_name = None
    new_business_flag = None
    full_address_string = None
    business_name = None
    establishment = None
    
    if venue is None:
        if lat is None or lon is None:
            # Note: GPS extraction from stream would require the stream to contain EXIF data
            # This functionality may need to be implemented if GPS extraction from BytesIO is needed
            print({"error": "GPS coordinates not provided and cannot extract from stream."})
        else:
            full_address_string = get_street_address_from_lat_lon(lat, lon)
            print(f"Extracted GPS coordinates: Latitude={lat}, Longitude={lon}")
    
    if venue:
        # If venue is provided, use it
        print("Getting Venue Name......")
        venue_name = db_get_venue_name(venue)
        if venue_name:
            new_business_flag = False
            print(f"Using provided venue: {venue_name}")

    # Handle image conversion and upload
    try:
        # Set flag to handle corrupted image headers
        ImageFile.LOAD_TRUNCATED_IMAGES = True
        Image.ALLOW_INCORRECT_HEADERS = True
        Image.MAX_IMAGE_PIXELS = None
        
        # Reset stream position to beginning
        image_stream.seek(0)
        img = Image.open(image_stream)
        
        # Convert to RGB if necessary (for JPEG compatibility)
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        
        # Save as JPEG (most widely supported format)
        output_buffer = BytesIO()
        img.save(output_buffer, format='JPEG', quality=95)
        output_buffer.seek(0)
        
        # Create a temporary file and upload to blob storage
        with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as temp_file:
            temp_file.write(output_buffer.getvalue())
            temp_file_path = temp_file.name

        # Upload to blob storage
        try:
            # Generate blob name from filename or use UUID
            if filename:
                original_name = os.path.splitext(filename)[0]
                new_blob_name = f"{original_name}.jpg"
            else:
                new_blob_name = f"{str(uuid.uuid4())}.jpg"
            
            url = blb_upload_file_to_blob(
                file_path_or_stream=temp_file_path,
                container_name="directory-web-pages",
                blob_name=new_blob_name
            )
            print(f"File converted and uploaded successfully. Blob URL: {url}")
        except Exception as e:
            print(f"Error: {e}")

        # Clean up temporary file
        os.unlink(temp_file_path)

        # Use the blob URL as input
        image_input = url
    except Exception as e:
        return {"error": f"Failed to process image stream: {str(e)}"}

    # Get image category using the content understanding analyzer
    cu_response = cu_analyzer_main(image_input, "cu-initial-image-analyzer")
    if cu_response is None:
        return {"error": "Failed to process image or extract initial information."}
    
    # Handle case where cu_response might be a string instead of dict
    if isinstance(cu_response, str):
        return {"error": f"CU analyzer returned string instead of dict: {cu_response}"}
    
    category = cu_response.get("class")
    print(f"Image categorized as: {category}")

    session_uid = str(uuid.uuid4())

    # Step 2a - Analyze image according to category
    if category is None:
        return {"error": "Failed to determine image category."}
    elif category == "wifi_password":
        content_detail = cu_analyzer_main(image_input, "cu-wifi-password-analyzer")
        if content_detail is None:
            return {"error": "Failed to extract WiFi password details."}
    elif category == "hours_of_operation":
        content_detail = cu_analyzer_main(image_input, "cu-hours-of-operation")
        if content_detail is None:
            return {"error": "Failed to extract hours of operation details."}
    elif category == "tap_list":
        content_detail = cu_analyzer_main(image_input, "cu-tap-list-parser")
        if content_detail is None:
            return {"error": "Failed to extract tap list details."}
    elif category == "product_offerings":
        content_detail = cu_analyzer_main(image_input, "cu-product-offering-analyzer")
        if content_detail is None:
            return {"error": "Failed to extract product offerings details."}
    elif category == "business_general":
        # General business image - business type classification will happen for all, so not done here.
        pass

    prod_list = []
    venue_list = []

    business_type = cu_analyzer_main(image_input, "cu-business-type-classifier")
    if business_type is None:
        return {"error": "Failed to classify business type."}
    else:
        business_type_string = business_type.get("business_type")
        business_name = business_type.get("business_name")
        print(f"Business type classified as: {business_type_string}")
    
    # Step 2b - If GPS coordinates are available and venue is none, find nearby venues
    if lat is not None and lon is not None and venue is None:
        radius_miles = .1  # default search radius
        return_count = 3    # default number of results to return
        print(f"inputs: {business_type_string}, {lat}, {lon}, {radius_miles}, {return_count}")
        venue_list_data = db_get_business_list_by_lat_lon(business_type_string, lat, lon, radius_miles, return_count)
        if venue_list_data is None and new_business_flag is None:
            new_business_flag = True
            venue_list = [{"venue": venue, "venue_name": venue_name}] if venue and 'venue_name' in locals() else []
        elif venue_list_data:
            new_business_flag = False
            venue_list = [
                {"venue": record.get("venue"), "venue_name": record.get("venue_name")}
                for record in venue_list_data
                if "venue" in record and "venue_name" in record
            ] if venue_list_data else [{"venue": venue, "venue_name": venue_name}]
        else:
            new_business_flag = True

    print(f"Nearby venues found: {venue_list}")
    # Step 3 - Commit the gathered database data to Stage.

    if category is None:
        return {"error": "Failed to determine image category."}
    elif category == "wifi_password":
        wifi_network = content_detail.get("wifi_network")
        wifi_password = content_detail.get("wifi_password")
        print(f"wifi network and password: {wifi_network}, {wifi_password}")
        if not venue_list:
            return {"error": "No venues found to stage WiFi data."}
        else:
            stg_response = db_stage_wifi_data(session_uid, venue_list[0], wifi_network=wifi_network, wifi_password=wifi_password, content_url=image_input)
    elif category == "hours_of_operation":
        print(f"hours of operation: {content_detail}")
        monday_summary = content_detail.get("monday_hours_summary")
        monday_open = content_detail.get("monday_open_time")
        monday_close = content_detail.get("monday_close_time")
        tuesday_summary = content_detail.get("tuesday_hours_summary")
        tuesday_open = content_detail.get("tuesday_open_time")
        tuesday_close = content_detail.get("tuesday_close_time")
        wednesday_summary = content_detail.get("wednesday_hours_summary")
        wednesday_open = content_detail.get("wednesday_open_time")
        wednesday_close = content_detail.get("wednesday_close_time")
        thursday_summary = content_detail.get("thursday_hours_summary")
        thursday_open = content_detail.get("thursday_open_time")
        thursday_close = content_detail.get("thursday_close_time")
        friday_summary = content_detail.get("friday_hours_summary")
        friday_open = content_detail.get("friday_open_time")
        friday_close = content_detail.get("friday_close_time")
        saturday_summary = content_detail.get("saturday_hours_summary")
        saturday_open = content_detail.get("saturday_open_time")
        saturday_close = content_detail.get("saturday_close_time")
        sunday_summary = content_detail.get("sunday_hours_summary")
        sunday_open = content_detail.get("sunday_open_time")
        sunday_close = content_detail.get("sunday_close_time")
        start_date = content_detail.get("schedule_effective_start_date")
        end_date = content_detail.get("schedule_effective_end_date")
        
        if not content_detail:
            return {"error": "No content detail found to stage hours of operation data."}
        else:
            stg_response = db_stage_hours_of_operation_data(session_uid, venue,
                monday_summary, monday_open, monday_close,
                tuesday_summary, tuesday_open, tuesday_close,
                wednesday_summary, wednesday_open, wednesday_close,
                thursday_summary, thursday_open, thursday_close,
                friday_summary, friday_open, friday_close,
                saturday_summary, saturday_open, saturday_close,
                sunday_summary, sunday_open, sunday_close,
                start_date, end_date, image_input
                )
        print("placeholder")
    elif category == "tap_list":
        print("placeholder")
    elif category == "product_offerings":
        prod_list = extract_product_offering_list(content_detail) if 'content_detail' in locals() else None
        if isinstance(prod_list, list):
            prod_list = ', '.join(prod_list)
        db_response = db_stage_product_offering_data(session_uid=session_uid, is_new_business=new_business_flag, business_name=business_name,
                        business_type=business_type_string, latitude=lat, longitude=lon, full_address_string=full_address_string, venue=venue,
                        product_list=prod_list, content_url=image_input, stage_datetime=datetime.now())
        print(f"Extracted product offerings: {prod_list}")
    elif category == "business_general":
        # General business image - business type classification will happen for all, so not done here.
        pass
    
    return {
        "status": "Done",
        "session_uid": session_uid,
        "category": category,
        "business_type": business_type,
        "venue_list": venue_list,
        "product_list": prod_list,
        "content_detail": content_detail if 'content_detail' in locals() else None
    }


def img_initial_image_process(image_url: str, venue: Optional[str] = None, lat: Optional[float] = None, lon: Optional[float] = None):
    # Step 1 - Get Latitude and Longitude and classify the image
    
    venue_name = None
    new_business_flag = None
    full_address_string = None
    business_name = None
    establishment = None
    
    if venue is None:
        if lat is None or lon is None:
            lat, lon = get_gps_coordinates(image_url)
            if lat is None or lon is None:
                print({"error": "Failed to extract GPS coordinates from image."})
        else:
            full_address_string = get_street_address_from_lat_lon(lat, lon)
            print(f"Extracted GPS coordinates: Latitude={lat}, Longitude={lon}")
    
    if venue:
        # If venue is provided, use it
        print("Getting Venue Name......")
        venue_name = db_get_venue_name(venue)
        if venue_name:
            new_business_flag = False
            print(f"Using provided venue: {venue_name}")

    # Check file extension and convert if necessary

    parsed_url = urlparse(image_url)
    file_path = parsed_url.path
    file_ext = os.path.splitext(file_path)[1].lower()

    if file_ext not in ['.jpg', '.jpeg', '.png', '.bmp', '.heif']:
        try:
            # Download the image
            response = requests.get(image_url)
            response.raise_for_status()
            
            # Open and convert the image
            # Set flag to handle corrupted image headers
            from PIL import ImageFile
            ImageFile.LOAD_TRUNCATED_IMAGES = True
            Image.ALLOW_INCORRECT_HEADERS = True
            Image.MAX_IMAGE_PIXELS = None
            
            img = Image.open(BytesIO(response.content))
            
            # Convert to RGB if necessary (for JPEG compatibility)
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')
            
            # Save as JPEG (most widely supported format)
            output_buffer = BytesIO()
            img.save(output_buffer, format='JPEG', quality=95)
            output_buffer.seek(0)
            
            # Create a temporary file and upload to blob storage
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as temp_file:
                temp_file.write(output_buffer.getvalue())
                temp_file_path = temp_file.name

            # Upload to blob storage
            try:
                # Extract the original filename from the URL and change extension to .jpg
                original_filename = os.path.basename(parsed_url.path)
                original_name = os.path.splitext(original_filename)[0]
                new_blob_name = f"{original_name}.jpg"
                
                url = blb_upload_file_to_blob(
                    file_path_or_stream=temp_file_path,
                    container_name="directory-web-pages",
                    blob_name=new_blob_name
                )
                print(f"File converted and uploaded successfully. Blob URL: {url}")
            except Exception as e:
                print(f"Error: {e}")

            # Clean up temporary file
            os.unlink(temp_file_path)

            # Use the blob URL as input
            image_input = url
        except Exception as e:
            return {"error": f"Failed to convert image format: {str(e)}"}
    else:
        # Use the original image URL
        image_input = image_url

    # Get image category using the content understanding analyzer
    cu_response = cu_analyzer_main(image_input, "cu-initial-image-analyzer")
    if cu_response is None:
        return {"error": "Failed to process image or extract initial information."}
    # print(f"CU Initial Image Processing Complete - {cu_response}")
    
    # Handle case where cu_response might be a string instead of dict
    if isinstance(cu_response, str):
        return {"error": f"CU analyzer returned string instead of dict: {cu_response}"}
    
    category = cu_response.get("class")
    print(f"Image categorized as: {category}")

    session_uid = str(uuid.uuid4())

    # Step 2a - Analyze image according to category
    if category is None:
        return {"error": "Failed to determine image category."}
    elif category == "wifi_password":
        content_detail = cu_analyzer_main(image_input, "cu-wifi-password-analyzer")
        if content_detail is None:
            return {"error": "Failed to extract WiFi password details."}
    elif category == "hours_of_operation":
        content_detail = cu_analyzer_main(image_input, "cu-hours-of-operation")
        if content_detail is None:
            return {"error": "Failed to extract hours of operation details."}
    elif category == "tap_list":
        content_detail = cu_analyzer_main(image_input, "cu-tap-list-parser")
        if content_detail is None:
            return {"error": "Failed to extract tap list details."}
    elif category == "product_offerings":
        content_detail = cu_analyzer_main(image_input, "cu-product-offering-analyzer")
        if content_detail is None:
            return {"error": "Failed to extract product offerings details."}
    elif category == "business_general":
        # General business image - business type classification will happen for all, so not done here.
        pass

    prod_list = []
    venue_list = []

    business_type = cu_analyzer_main(image_input, "cu-business-type-classifier")
    if business_type is None:
        return {"error": "Failed to classify business type."}
    else:
        business_type_string = business_type.get("business_type")
        business_name = business_type.get("business_name")
        print(f"Business type classified as: {business_type_string}")
    
    # Step 2b - If GPS coordinates are available and venue is none, find nearby venues
    if lat is not None and lon is not None and venue is None:
        radius_miles = .1  # default search radius
        return_count = 3    # default number of results to return
        print(f"inputs: {business_type_string}, {lat}, {lon}, {radius_miles}, {return_count}")
        venue_list_data = db_get_business_list_by_lat_lon(business_type_string, lat, lon, radius_miles, return_count)
        if venue_list_data is None and new_business_flag is None:
            new_business_flag = True
            venue_list = [{"venue": venue, "venue_name": venue_name}] if venue and 'venue_name' in locals() else []
        elif venue_list_data:
            new_business_flag = False
            venue_list = [
                {"venue": record.get("venue"), "venue_name": record.get("venue_name")}
                for record in venue_list_data
                if "venue" in record and "venue_name" in record
            ] if venue_list_data else [{"venue": venue, "venue_name": venue_name}]
        else:
            new_business_flag = True

    print(f"Nearby venues found: {venue_list}")
    # Step 3 - Commit the gathered database data to Stage.

    if category is None:
        return {"error": "Failed to determine image category."}
    elif category == "wifi_password":
        wifi_network = content_detail.get("wifi_network")
        wifi_password = content_detail.get("wifi_password")
        print(f"wifi network and password: {wifi_network}, {wifi_password}")
        if not venue_list:
            return {"error": "No venues found to stage WiFi data."}
        else:
            stg_response = db_stage_wifi_data(session_uid, venue_list[0], wifi_network=wifi_network, wifi_password=wifi_password, content_url=image_input)
    elif category == "hours_of_operation":
        print(f"hours of operation: {content_detail}")
        monday_summary = content_detail.get("monday_hours_summary")
        monday_open = content_detail.get("monday_open_time")
        monday_close = content_detail.get("monday_close_time")
        tuesday_summary = content_detail.get("tuesday_hours_summary")
        tuesday_open = content_detail.get("tuesday_open_time")
        tuesday_close = content_detail.get("tuesday_close_time")
        wednesday_summary = content_detail.get("wednesday_hours_summary")
        wednesday_open = content_detail.get("wednesday_open_time")
        wednesday_close = content_detail.get("wednesday_close_time")
        thursday_summary = content_detail.get("thursday_hours_summary")
        thursday_open = content_detail.get("thursday_open_time")
        thursday_close = content_detail.get("thursday_close_time")
        friday_summary = content_detail.get("friday_hours_summary")
        friday_open = content_detail.get("friday_open_time")
        friday_close = content_detail.get("friday_close_time")
        saturday_summary = content_detail.get("saturday_hours_summary")
        saturday_open = content_detail.get("saturday_open_time")
        saturday_close = content_detail.get("saturday_close_time")
        sunday_summary = content_detail.get("sunday_hours_summary")
        sunday_open = content_detail.get("sunday_open_time")
        sunday_close = content_detail.get("sunday_close_time")
        start_date = content_detail.get("schedule_effective_start_date")
        end_date = content_detail.get("schedule_effective_end_date")
        
        if not content_detail:
            return {"error": "No content detail found to stage hours of operation data."}
        else:
            stg_response = db_stage_hours_of_operation_data(session_uid, venue,
                monday_summary, monday_open, monday_close,
                tuesday_summary, tuesday_open, tuesday_close,
                wednesday_summary, wednesday_open, wednesday_close,
                thursday_summary, thursday_open, thursday_close,
                friday_summary, friday_open, friday_close,
                saturday_summary, saturday_open, saturday_close,
                sunday_summary, sunday_open, sunday_close,
                start_date, end_date, image_input
                )
        print("placeholder")
    elif category == "tap_list":
        print("placeholder")
    elif category == "product_offerings":
        prod_list = extract_product_offering_list(content_detail) if 'content_detail' in locals() else None
        if isinstance(prod_list, list):
            prod_list = ', '.join(prod_list)
        db_response = db_stage_product_offering_data(session_uid=session_uid, is_new_business=new_business_flag, business_name=business_name,
                        business_type=business_type_string, latitude=lat, longitude=lon, full_address_string=full_address_string, venue=venue,
                        product_list=prod_list, content_url=image_url, stage_datetime=datetime.now())
        print(f"Extracted product offerings: {prod_list}")
    elif category == "business_general":
        # General business image - business type classification will happen for all, so not done here.
        pass
    
    return {
        "status": "Done",
        "session_uid": session_uid,
        "category": category,
        "business_type": business_type,
        "venue_list": venue_list,
        "product_list": prod_list,
        "content_detail": content_detail if 'content_detail' in locals() else None
    }

def extract_product_offering_list(cu_response):
    found_items = []
    for key, value in cu_response.items():
        if key.endswith('_yn') and value is True:
            # Remove the '_yn' suffix and replace underscores with spaces
            item_name = key[:-3].replace('_', ' ')
            found_items.append(item_name)
    return found_items

if __name__ == "__main__":
    # content_url = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/new-business/IMG_8044_crop2.heic"
    content_url = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/new-business/farmstand-example-1.jpg"
    # content_url = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/wifi-password/Truenorthwifi_pass_without_venue.HEIC"
    venue = 'chrisfarmstand.com#haverhill#web'
    # venue = None
    # content_url = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/wifi-password/Fathenwifi_pass_with_venue.HEIC"
    # venue = 'VNU#fathenbrewingcompany.com#web'
    # content_url = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/hours-of-operation/barreled_souls_base.png"
    # content_url = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/hours-of-operation/barreled_souls_20250616_20250622.png"
    #  venue = "VNU#barreledsouls.com#web"
    resp = img_initial_image_process(content_url, venue)
    # session_uid = ""
    # resp = db_load_stage_to_hours_of_operation(session_uid)
    print(resp)
    # print("Processing complete.")
