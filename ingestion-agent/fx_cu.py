import time
import requests
import json
import os
from dotenv import load_dotenv


def cu_analyzer_main(blob_url, analyzer_name):
    # Load environment variables from .env file
    load_dotenv()

    # Get the subscription key from environment variables
    subscription_key = os.getenv('COGNITIVE_SERVICE_SUBSCRIPTION_KEY')
    if not subscription_key:
        raise ValueError("COGNITIVE_SERVICE_SUBSCRIPTION_KEY not found in environment variables")
    content_understanding_endpoint = os.getenv('COGNITIVE_SERVICE_ENDPOINT')
    content_understanding_analyzer = analyzer_name
    content_url = blob_url
    # print(f"Using analyzer: {content_understanding_analyzer}")

    request_id_from_post = cu_api_post_request(content_understanding_endpoint, content_understanding_analyzer, content_url, subscription_key)
    # print(f"Request ID: {request_id_from_post}")

    if request_id_from_post:
        # print(f"Request ID: {request_id_from_post}")
        cu_result = cu_api_get_result_request(content_understanding_endpoint, subscription_key, content_url, request_id_from_post)
        if analyzer_name == "test":
            return "test placeholder"
        elif analyzer_name == "cu-initial-image-analyzer":
            classify_response = extract_cu_json_classify_response(cu_result)
            return classify_response
        elif analyzer_name == "cu-wifi-password-analyzer":
            wifi_response = extract_cu_json_wifi_response(cu_result)
            return wifi_response
        elif analyzer_name == "cu-hours-of-operation":
            hoursofoperation_response = extract_cu_json_hours_of_operation_response(cu_result)
            return hoursofoperation_response
        elif analyzer_name == "cu-product-offering-analyzer":
            product_offering_response = extract_cu_product_offering(cu_result)
            return product_offering_response
        elif analyzer_name == "cu-business-type-classifier":
            classify_response = extract_cu_json_classify_response(cu_result)
            return classify_response
        elif analyzer_name == "cu-business-type-from-webpage-analyzer":
            classify_response = extract_cu_json_classify_response(cu_result)
            return classify_response
        elif analyzer_name == "cua-beer-list":
            beerlist_response = extract_cu_json_output_list(cu_result, content_url)
            return beerlist_response
        elif analyzer_name == "cua-web-directory-page":
            webdir_response = extract_cu_json_output_list(cu_result, content_url)
            return webdir_response
        elif analyzer_name == "cua-untappd-checkin-list":
            untappd_checkin_response = extract_cu_json_output_list(cu_result, content_url)
            return untappd_checkin_response
        else:
            return "Failed to retrieve results."
    else:
        return "Failed to get back request ID."


def cu_api_post_request(cu_endpoint, analyzer_name, content_url, subscription_key):
    # print(f"Using endpoint: {cu_endpoint}")
    print(f"Using analyzer: {analyzer_name}")
    # print(f"Using content URL: {content_url}")
    # print(f"Using subscription key: {subscription_key}")

    post_api_url = f"{cu_endpoint}/contentunderstanding/analyzers/{analyzer_name}:analyze?api-version=2025-05-01-preview"
    header_data = {
        'Ocp-Apim-Subscription-Key': subscription_key,
        'Content-Type': 'application/json'
    }
    body_data = {
        "url": content_url
    }

    try:
        post_response = requests.post(post_api_url, headers=header_data, json=body_data)
        if post_response is None:
            print("No response received.")
        post_response.raise_for_status()
        post_response_data = post_response.json()
        request_id = post_response_data.get('id')
        return request_id
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None

def cu_api_get_result_request(cu_endpoint, subscription_key, content_url, request_id):
    get_api_request_result_url = f"{cu_endpoint}/contentunderstanding/analyzerResults/{request_id}?api-version=2025-05-01-preview"
    get_header_data = {
        'Ocp-Apim-Subscription-Key': subscription_key
    }
    status = "Running"
    iteration_count = 1
    max_iterations = 50  # Limit to avoid infinite loop
    while status == "Running" and iteration_count < max_iterations:
        try:
            get_response = requests.get(get_api_request_result_url, headers=get_header_data)
            get_response.raise_for_status()
            status = get_response.json().get("status", "")
            print(f"Current status: {status} - Iteration: {iteration_count}")
            if status == "Succeeded":
                # Extract business information from the response
                response_data = get_response.json()
                return response_data
            elif status != "Running":
                print("Request failed.")
                return "Request Failed."
            iteration_count += 1
            time.sleep(5)  # Wait before next check
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return None

def extract_cu_json_wifi_response(response_data):
    wifi_info = {}
    if 'result' in response_data and 'contents' in response_data['result'] and len(response_data['result']['contents']) > 0:
        content = response_data['result']['contents'][0]
        if 'fields' in content:
            wifi_network = content['fields']['wifi_network_name'].get('valueString', {})
            wifi_password = content['fields']['wifi_password'].get('valueString', {})
            wifi_info = {
                "wifi_network": wifi_network,
                "wifi_password": wifi_password
            }
    return wifi_info
    # return False

def extract_cu_json_hours_of_operation_response(response_data):
    hours_info = {}
    if 'result' in response_data and 'contents' in response_data['result'] and len(response_data['result']['contents']) > 0:
        content = response_data['result']['contents'][0]
        if 'fields' in content:
            monday_hours_summary = content['fields']['monday_hours_summary'].get('valueString', {})
            monday_open_time = content['fields']['monday_open_time'].get('valueTime', {})
            monday_close_time = content['fields']['monday_close_time'].get('valueTime', {})
            tuesday_hours_summary = content['fields']['tuesday_hours_summary'].get('valueString', {})
            tuesday_open_time = content['fields']['tuesday_open_time'].get('valueTime', {})
            tuesday_close_time = content['fields']['tuesday_close_time'].get('valueTime', {})
            wednesday_hours_summary = content['fields']['wednesday_hours_summary'].get('valueString', {})
            wednesday_open_time = content['fields']['wednesday_open_time'].get('valueTime', {})
            wednesday_close_time = content['fields']['wednesday_close_time'].get('valueTime', {})
            thursday_hours_summary = content['fields']['thursday_hours_summary'].get('valueString', {})
            thursday_open_time = content['fields']['thursday_open_time'].get('valueTime', {})
            thursday_close_time = content['fields']['thursday_close_time'].get('valueTime', {})
            friday_hours_summary = content['fields']['friday_hours_summary'].get('valueString', {})
            friday_open_time = content['fields']['friday_open_time'].get('valueTime', {})
            friday_close_time = content['fields']['friday_close_time'].get('valueTime', {})
            saturday_hours_summary = content['fields']['saturday_hours_summary'].get('valueString', {})
            saturday_open_time = content['fields']['saturday_open_time'].get('valueTime', {})
            saturday_close_time = content['fields']['saturday_close_time'].get('valueTime', {})
            sunday_hours_summary = content['fields']['sunday_hours_summary'].get('valueString', {})
            sunday_open_time = content['fields']['sunday_open_time'].get('valueTime', {})
            sunday_close_time = content['fields']['sunday_close_time'].get('valueTime', {})
            schedule_effective_start_date = content['fields']['schedule_effective_start_date'].get('valueDate', {})
            schedule_effective_end_date = content['fields']['schedule_effective_end_date'].get('valueDate', {})
            hours_info = {
                "monday_hours_summary": monday_hours_summary,
                "monday_open_time": monday_open_time,
                "monday_close_time": monday_close_time,
                "tuesday_hours_summary": tuesday_hours_summary,
                "tuesday_open_time": tuesday_open_time,
                "tuesday_close_time": tuesday_close_time,
                "wednesday_hours_summary": wednesday_hours_summary,
                "wednesday_open_time": wednesday_open_time,
                "wednesday_close_time": wednesday_close_time,
                "thursday_hours_summary": thursday_hours_summary,
                "thursday_open_time": thursday_open_time,
                "thursday_close_time": thursday_close_time,
                "friday_hours_summary": friday_hours_summary,
                "friday_open_time": friday_open_time,
                "friday_close_time": friday_close_time,
                "saturday_hours_summary": saturday_hours_summary,
                "saturday_open_time": saturday_open_time,
                "saturday_close_time": saturday_close_time,
                "sunday_hours_summary": sunday_hours_summary,
                "sunday_open_time": sunday_open_time,
                "sunday_close_time": sunday_close_time,
                "schedule_effective_start_date": schedule_effective_start_date,
                "schedule_effective_end_date": schedule_effective_end_date
            }
    return hours_info

def extract_cu_json_classify_response(response_data):
    classify_info = {}
    if 'result' in response_data and 'contents' in response_data['result'] and len(response_data['result']['contents']) > 0:
        content = response_data['result']['contents'][0]
        if 'fields' in content:
            classify_info = {key: value.get('valueString', {}) for key, value in content['fields'].items()}
    return classify_info
    # return False

def extract_cu_json_output_list(response_data, content_url):
    output_list = []
    print(response_data)
    # Navigate to the website_list array
    if 'result' in response_data and 'contents' in response_data['result'] and len(response_data['result']['contents']) > 0:
        content = response_data['result']['contents'][0]
        if 'fields' in content and 'output_list' in content['fields']:
            output_records_list = content['fields']['output_list']
            if 'valueArray' in output_records_list:
                for item in output_records_list['valueArray']:
                    # Create a new dictionary for each item to avoid reference issues
                    output_list_info = {
                        "raw_item_json": item,
                        "source": content_url,
                        "pull_datetime": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                    }
                    # Add to array if we have both fields
                    if 'raw_item_json' in output_list_info and 'source' in output_list_info:
                        output_list.append(output_list_info)

    print(f"Extracted {len(output_list)} list entries:")
    return output_list

def extract_cu_product_offering(response_data):
    product_info = {}
    if 'result' in response_data and 'contents' in response_data['result'] and len(response_data['result']['contents']) > 0:
        content = response_data['result']['contents'][0]
        if 'fields' in content:
            for field_name, field_data in content['fields'].items():
                if field_data.get('type') == 'boolean':
                    product_info[field_name] = field_data.get('valueBoolean', False)
                elif field_data.get('type') == 'number':
                    product_info[field_name] = field_data.get('valueNumber', 0)
                else:
                    product_info[field_name] = field_data.get('valueString', '')
    return product_info
    # return False

# def get_cu_client():
#     settings = Settings(
#         endpoint="https://campio-2025-fl-jsa.services.ai.azure.com/",
#         api_version="2025-05-01-preview",
#         # Either subscription_key or aad_token must be provided. Subscription Key is more prioritized.
#         subscription_key="AZURE_CONTENT_UNDERSTANDING_SUBSCRIPTION_KEY",
#         aad_token="AZURE_CONTENT_UNDERSTANDING_AAD_TOKEN",
#         # Insert the analyzer name.
#         analyzer_id="cu-wifi-password-analyzer",
#         # Insert the supported file types of the analyzer.
#         file_location="https://raw.githubusercontent.com/Azure/azure-sdk-for-python/main/sdk/formrecognizer/azure-ai-formrecognizer/tests/sample_forms/receipt/contoso-receipt.png",
#         )
#     client = AzureContentUnderstandingClient(
#         settings.endpoint,
#         settings.api_version,
#         subscription_key=settings.subscription_key,
#         token_provider=settings.token_provider,
#     )
#     return client
  

if __name__ == "__main__":
    content_understanding_analyzer = "cu-business-type-from-webpage-analyzer"
    content_url = "https://campio2025flmobile.blob.core.windows.net/scraped-content/scraped_html_1756241502.html"
    cu_response = cu_analyzer_main(content_url, content_understanding_analyzer)
    print(cu_response)
    
#     venue = "VNU#truenorthales.com#web"
#     # request_id = "e8fb6b7c-72dc-4746-8ea8-2ad4393e1790"
#     # response = cu_api_post_request(COGNITIVE_SERVICE_ENDPOINT, content_understanding_analyzer, content_url, COGNITIVE_SERVICE_SUBSCRIPTION_KEY)
#     # response = cu_api_get_result_request(COGNITIVE_SERVICE_ENDPOINT, COGNITIVE_SERVICE_SUBSCRIPTION_KEY, content_url, request_id)
#     cls = response.get("class")
#     print(f"Classified as: {cls}")

