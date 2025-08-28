from typing import Optional
from fx_cu import cu_analyzer_main
from fx_db import db_upsert_wifi_network_password, db_image_to_content_upsert, db_output_list_to_stage, db_get_business_list_by_lat_lon
from fx_db import db_load_stage_to_hours_of_operation, db_load_stage_to_wifi
from fx_image_processing import img_initial_image_process
from fx_utilities import get_gps_coordinates
import os
from urllib.parse import urlparse
from PIL import Image
import requests
from io import BytesIO
import base64
import tempfile

def initial_image_process(image_url: str, venue: Optional[str] = None):
    resp = img_initial_image_process(image_url, venue)
    print(f"Initial Image Processing - status: {resp.get('status', 'Unknown')}")
    print(f"Initial Image Processing - category: {resp.get('category', 'Unknown')}")
    print(f"Initial Image Processing - business_type: {resp.get('business_type', 'Unknown')}")
    print(f"Initial Image Processing - product_list: {resp.get('product_list', [])}")
    print(f"Initial Image Processing - venue_list: {resp.get('venue_list', [])}")
    print(f"Initial Image Processing - content_detail: {resp.get('content_detail', [])}")
    return resp

def process_confirm_commit_hil(session_uid: str, process_hil: str):
    if process_hil == "hours_of_operation":
        resp = db_load_stage_to_hours_of_operation(session_uid)
    elif process_hil == "wifi_password":
        resp = db_load_stage_to_wifi(session_uid)
    elif process_hil == "tap_list":
        print("placeholder")
        # resp = db_load_stage_to_tap_list(session_uid)
    elif process_hil == "product_offerings":
        print("placeholder")
        # resp = db_load_stage_to_product_offerings(session_uid)
    elif process_hil == "business_general":
        print("placeholder")
        # resp = db_load_stage_to_business_general(session_uid)
    return resp

def process_untappd_checkins(blob_url: str):
    cu_response = cu_analyzer_main(blob_url, "cua-untappd-checkin-list")
    if cu_response is None:
        return {"error": "Failed to process image or extract Untappd check-in information."}
    print("CU Untappd Check-ins Processing Complete - Database Load Next")
    # Process the CU response and load into the database
    stage_response = db_output_list_to_stage(cu_response, "cua-untappd-checkin-list")
    return {"message": "Untappd Check-ins processed successfully."}

def process_hours_of_operation_image(blob_url: str, venue: Optional[str] = None, latitude: Optional[float] = None, longitude: Optional[float] = None):
    print(f"Processing hours of operation image from blob URL: {blob_url} in venue: {venue}")
    
    
def process_wifi_password_image(blob_url: str, venue: str):
    cu_response = cu_analyzer_main(blob_url, "cu-wifi-password-analyzer")
    if cu_response is None:
        return {"error": "Failed to process image or extract WiFi information."}
    print("CU Wifi Processing Complete - Database Load Next")
    WiFi_Network = cu_response.get("wifi_network")
    WiFi_Password = cu_response.get("wifi_password")
    if not WiFi_Network or not WiFi_Password:
        return {"error": "Failed to extract WiFi information from image."}
    db_pwd_response = db_upsert_wifi_network_password(venue, WiFi_Network, WiFi_Password, True, True, "MCP Server")
    if db_pwd_response is None:
        return {"error": "Failed to upsert WiFi information into the database."}
    print("WiFi Information Upserted - Content Upsert Next")
    homepage_url = venue.replace("VNU#", "").replace("#WEB", "")
    db_cntnt_response = db_image_to_content_upsert(blob_url, "WiFi", "Venue", homepage_url)
    if db_cntnt_response is None:
        return {"error": "Failed to upsert content information into the database."}
    print("Content Upserted Successfully")
    return {"Wifi Image processed into database"}

# if __name__ == "__main__":
#     # egg stand
#     # content_url = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/new-business/IMG_8044.HEIC"
#     # brewery wifi - With Venue
#     content_url = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/hours-of-operation/barreled_souls_20250616_20250622.png"
#     venue = "VNU#barreledsouls.com#web"
#     resp = initial_image_process(content_url, venue)
#     # resp = initial_image_process(content_url, venue)
#     # session_uid = '03627dab-4db9-4052-b724-ff6459871fe4'
#     # resp = db_load_stage_to_hours_of_operation(session_uid)
#     print(resp)
    
#     print("Processing complete.")
