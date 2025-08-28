from fx_orchestration import process_wifi_password_image, process_confirm_commit_hil, initial_image_process
from fx_db import db_image_to_content_upsert, db_get_business_list_by_lat_lon, db_create_new_business_from_image, db_get_business_types
from fx_blb import blb_upload_file_to_blob, blb_get_blob_size
from fx_cu import cu_analyzer_main
from fastapi import FastAPI, HTTPException
from fastapi_mcp import FastApiMCP
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import uvicorn
from typing import Optional

app = FastAPI()

@app.get("/", response_class=PlainTextResponse)
def home():
    return "Hello, FastAPI!"

class ImageData(BaseModel):
    image_url: str
    venue: Optional[str] = None

@app.post("/api_initial_image_process", operation_id="initial_image_process")
async def api_initial_image_process(data: ImageData):
    """
    Processes an image by analyzing its content and returns findings for confirmation.

    This asynchronous function is designed for use within the MCP Server backend. It accepts a request object containing the necessary parameters to process a WiFi password image, including the blob URL and venue information. The function interacts with both the content understanding and database modules to perform the required operations.

    Args:
        image_url (str): The URL of the image to be processed.

    Returns:
        dict: The response from the processing operation.
    """
    image_url_str=data.image_url
    venue_str = data.venue

    # Ensure parameter order and types match the SQL procedure/query expectations
    try:
        if venue_str is not None and venue_str.strip() == "":
            venue_str = None
        if image_url_str is None or image_url_str.strip() == "":
            image_url_str = None
        if image_url_str is not None:
            resp = initial_image_process(image_url_str, venue_str)
            return resp
        else:
            return {"error": "Invalid input parameters."}
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}

@app.post("/api_confirm_commit_hil", operation_id="confirm_commit_hil")
async def api_confirm_commit_hil(session_uid: str, process_hil: str):
    """
    When a separate process has brought data back to the user for confirmation, 
        this endpoint can be called to commit the processed data to the database.

    Args:
        session_uid (str): The unique identifier for the session.
        process_hil (str): The name of the process that is leveraging Human in the Loop (HIL) for confirmation.

    Returns:
        dict: The response from the processing operation.
    """
    resp = process_confirm_commit_hil(session_uid, process_hil)
    return resp


@app.get("/api_process_wifi_password_image", operation_id="process_wifi_password_image")
async def api_process_wifi_password_image(blob_url: str, venue: str):
    """
    Processes a WiFi password image by analyzing its content and updating the database.

    This asynchronous function is designed for use within the MCP Server backend. It accepts a request object containing the necessary parameters to process a WiFi password image, including the blob URL and venue information. The function interacts with both the content understanding and database modules to perform the required operations.

    Args:
        blob_url (str): The URL of the blob containing the WiFi password image.
        venue (str): The venue information associated with the WiFi password.

    Returns:
        dict: The response from the processing operation.
    """
    return process_wifi_password_image(blob_url, venue)

@app.get("/api_process_hours_of_operation_image", operation_id="process_hours_of_operation_image")
async def api_process_hours_of_operation_image(blob_url: str, venue: Optional[str] = None, latitude: Optional[float] = None, longitude: Optional[float] = None):
    """
    Processes a hours of operation image by analyzing its content and updating the database.

    This asynchronous function is designed for use within the MCP Server backend. It accepts a request object containing the necessary parameters to process a WiFi password image, including the blob URL and venue information. The function interacts with both the content understanding and database modules to perform the required operations.

    Args:
        blob_url (str): The URL of the blob containing the WiFi password image.
        venue (Optional[str]): The venue information associated with the WiFi password.
        longitude (Optional[float]): The longitude coordinate for the venue.
        latitude (Optional[float]): The latitude coordinate for the venue.

    Returns:
        dict: The response from the processing operation.
    """
    return process_wifi_password_image(blob_url, venue)

@app.post("/api_db_get_business_types", operation_id="db_get_business_types")
async def api_db_get_business_types():
    """
    Retrieves a list of business types that currently exist in the database.

    This asynchronous function is designed for use within the MCP Server backend. There are no parameters required for this request. The function queries the database for distinct business types.

    Args:
        None

    Returns:
        List[business_types]: A list of business types as string values.  The return is just a list of the strings.

    Raises:
        Any exceptions raised by the underlying database query function.
    Usage:
        This endpoint is typically used to power location-based business search features in MCP Server applications.
    """

    # Ensure parameter order and types match the SQL procedure/query expectations
    try:
        resp = db_get_business_types()
        return resp
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}

class CreateNewBusinessFromImageRequest(BaseModel):
    category: str
    latitude: float
    longitude: float
    image_url: str
    business_name: Optional[str] = None

@app.post("/api_db_create_new_business_from_image", operation_id="db_create_new_business_from_image")
async def api_db_create_new_business_from_image(data: CreateNewBusinessFromImageRequest):
    """
    Creates a new business entry in the database using an image.

    This asynchronous function is designed for use within the MCP Server backend. It accepts a request object containing the necessary parameters to create a new business entry, including the business category, geographic coordinates (latitude and longitude), image URL, and an optional business name. The function interacts with the database to insert the new business record.

    Args:
        data (CreateNewBusinessFromImageRequest): An object containing the following fields:
            - category (str): The business category for the new entry.
            - latitude (float): The latitude coordinate for the new business location.
            - longitude (float): The longitude coordinate for the new business location.
            - image_url (str): The URL of the image to associate with the new business.
            - business_name (Optional[str]): The name of the new business (if available).

    Returns:
        dict: A response object indicating the success or failure of the operation.
    """

    category=data.category
    latitude=float(data.latitude)
    longitude=float(data.longitude)
    image_url=data.image_url
    business_name=data.business_name

    # Ensure parameter order and types match the SQL procedure/query expectations
    try:
        resp = db_create_new_business_from_image(
            category,
            latitude,
            longitude,
            image_url,
            business_name
        )
        return resp
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}

class BusinessListByLatLonRequest(BaseModel):
    category: str
    latitude: float
    longitude: float
    radius_miles: Optional[float] = 10
    return_count: Optional[int] = 10

@app.post("/api_db_get_business_list_by_lat_lon", operation_id="db_get_business_list_by_lat_lon")
async def api_db_get_business_list_by_lat_lon(data: BusinessListByLatLonRequest):
    """
    Retrieves a list of businesses within a specified radius of a given latitude and longitude.

    This asynchronous function is designed for use within the MCP Server backend. It accepts a request object containing search parameters such as business category, geographic coordinates (latitude and longitude), search radius in miles, and the maximum number of results to return. The function queries the database for businesses matching the criteria and returns the resulting list.

    Args:
        data (BusinessListByLatLonRequest): An object containing the following fields:
            - category (str): The business category to filter results.
            - latitude (float): The latitude coordinate for the search center.
            - longitude (float): The longitude coordinate for the search center.
            - radius_miles (float): The search radius in miles.
            - return_count (int): The maximum number of businesses to return.

    Returns:
        List[Business]: A list of business objects that match the search criteria.
        Each business in the returned list contains the following fields:
            - Venue_name (str): The name of the business venue.
            - full_address (str): The complete address of the business.
            - distance (float): The distance from the search center to the business, in miles.
            - image_url (str): A URL to the business's logo or representative image.
    Raises:
        Any exceptions raised by the underlying database query function.
    Usage:
        This endpoint is typically used to power location-based business search features in MCP Server applications.
    """

    category=data.category
    latitude=float(data.latitude)
    longitude=float(data.longitude)
    if data.radius_miles:
        radius_miles=float(data.radius_miles)
    else:
        radius_miles=None
    if data.return_count:
        return_count=int(data.return_count)
    else:
        return_count=None
    # Ensure parameter order and types match the SQL procedure/query expectations
    try:
        resp = db_get_business_list_by_lat_lon(
            category,
            latitude,
            longitude,
            radius_miles,
            return_count
        )
        return resp
    except Exception as e:
        return {"error": f"Database error: {str(e)}"}


class ImageToContentUpsertRequest(BaseModel):
    image_url: str
    image_type: str
    homepage_url: str

@app.post("/api_db_image_to_content_upsert", operation_id="db_image_to_content_upsert")
async def api_image_to_content_upsert(data: ImageToContentUpsertRequest):
    """
    Upserts image content into the database for use within the MCP Server backend.

    This endpoint is designed to be called by MCP Server tools or workflows. It accepts an image URL, image type, and homepage URL, and stores or updates the corresponding content record in the database.

    Args:
        data (ImageToContentUpsertRequest): An object containing:
            - image_url (str): The URL of the image to upsert.
            - image_type (str): The type/category of the image.
            - homepage_url (str): The homepage URL associated with the image.

    Returns:
        The result of the upsert operation as returned by the database function.
    """
    resp = db_image_to_content_upsert(data.image_url, data.image_type, data.homepage_url)
    return resp

class UploadFileToBlobRequest(BaseModel):
    file_path: str
    container_name: str
    blob_name: str
    overwrite: bool = True

@app.post("/api_blb_upload_file_to_blob", operation_id="blb_upload_file_to_blob")
def api_blb_upload_file_to_blob(data: UploadFileToBlobRequest):
    """
    Uploads a file to Azure Blob Storage for use within the MCP Server.

    Args:
        data (UploadFileToBlobRequest): An object containing the following attributes:
            - file_path (str or file-like): The path to the file or a file-like stream to upload.
            - container_name (str): The name of the Azure Blob Storage container.
            - blob_name (str): The desired name of the blob in the container.
            - overwrite (bool): Whether to overwrite the blob if it already exists.

    Returns:
        dict: A dictionary containing the URL of the uploaded blob with the key 'blob_url'.

    Raises:
        AzureError: If the upload to Azure Blob Storage fails.
    """
    blob_url = blb_upload_file_to_blob(
        file_path_or_stream=data.file_path,
        container_name=data.container_name,
        blob_name=data.blob_name,
        overwrite=data.overwrite
    )
    return {"blob_url": blob_url}

class GetBlobSizeRequest(BaseModel):
    container_name: str
    blob_name: str

@app.get("/api_blb_get_blob_size")
def api_blb_get_blob_size(container_name: str, blob_name: str):
    """Get the size of a blob in Azure Blob Storage."""
    if not container_name or not blob_name:
        raise HTTPException(status_code=400, detail="Missing parameters")
    try:
        size = blb_get_blob_size(container_name, blob_name)
        return {"size": size}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting blob size: {str(e)}")


if __name__ == '__main__':
    mcp = FastApiMCP(app, include_operations=["initial_image_process", "db_get_business_list_by_lat_lon", "db_image_to_content_upsert", "blb_upload_file_to_blob", "confirm_commit_hil"],auth_config=None)
    # Mount the MCP server directly to your FastAPI app
    mcp.mount_http()
    uvicorn.run(app, host='0.0.0.0', port=80)