# importing geopy library and Nominatim class
import requests
from geopy.geocoders import Nominatim
import requests
from io import BytesIO
from PIL import Image
from PIL.ExifTags import TAGS
from pillow_heif import register_heif_opener

# Register HEIF opener to enable HEIC support
register_heif_opener()

def get_lat_lon(address_string: str):
    # calling the Nominatim tool and create Nominatim class
    loc = Nominatim(user_agent="Geopy Library")

    # entering the location name
    getLoc = loc.geocode(address_string)

    # printing address
    print(getLoc.address)
    latitude = getLoc.latitude
    longitude = getLoc.longitude

    return {"latitude": latitude, "longitude": longitude}

def get_street_address_from_lat_lon(latitude: float, longitude: float):
    # calling the Nominatim tool and create Nominatim class
    loc = Nominatim(user_agent="Geopy Library")

    # entering the location name
    getLoc = loc.reverse((latitude, longitude))

    # printing address
    full_address_string = getLoc.address
    if full_address_string:
        return full_address_string
    else:
        return None

def get_street_address(business_name: str, city: str, state: str):
    url = "https://api.opencorporates.com/v0.4/companies/search"
    params = {
        "q": business_name,
        "jurisdiction_code": f"us_{state.lower()}",
        "per_page": 10  # Get multiple results to filter by city
    }
    
    response = requests.get(url, params=params)
    data = response.json()

    if "results" in data and "companies" in data["results"]:
        for company_info in data["results"]["companies"]:
            company = company_info["company"]
            address = company.get("registered_address")
            if address and city.lower() in address.lower():
                return address

    return None

def get_gps_coordinates(image_path):
    """
    Extract GPS coordinates (latitude, longitude) from image EXIF data.
    Supports both local files and URLs.
    
    Args:
        image_path (str): Path to the image file or URL
        
    Returns:
        tuple: (latitude, longitude) as floats, or (None, None) if no GPS data found
    """
    try:
        # Check if it's a URL
        if image_path.startswith(('http://', 'https://')):
            # Download the image
            response = requests.get(image_path)
            response.raise_for_status()  # Raise an exception for bad status codes
            image_data = BytesIO(response.content)
            image = Image.open(image_data)
        else:
            # Open local file
            image = Image.open(image_path)
        
        with image:
            exif_data = image.getexif()
            
            if exif_data is None:
                return None, None
            
            # Find GPS info in EXIF data
            gps_info = None
            for tag, value in exif_data.items():
                tag_name = TAGS.get(tag, tag)
                if tag_name == 'GPSInfo':
                    # Get the GPS IFD (Image File Directory)
                    gps_info = image.getexif().get_ifd(tag)
                    break
            
            if gps_info is None:
                return None, None
            
            # Parse GPS coordinates
            lat = _get_decimal_from_dms(gps_info.get(2), gps_info.get(1))
            lon = _get_decimal_from_dms(gps_info.get(4), gps_info.get(3))
            
            return lat, lon
            
    except Exception as e:
        print(f"Error reading GPS data from {image_path}: {e}")
        return None, None 

def _get_decimal_from_dms(dms, ref):
    """
    Convert degrees, minutes, seconds to decimal degrees.
    
    Args:
        dms: Tuple of (degrees, minutes, seconds) as fractions
        ref: Reference direction ('N', 'S', 'E', 'W')
        
    Returns:
        float: Decimal degrees, or None if conversion fails
    """
    try:
        if dms is None or ref is None:
            return None
            
        degrees = float(dms[0])
        minutes = float(dms[1]) / 60.0
        seconds = float(dms[2]) / 3600.0
        
        decimal = degrees + minutes + seconds
        
        # Apply negative sign for South and West
        if ref in ['S', 'W']:
            decimal = -decimal
            
        return decimal
    except (TypeError, IndexError, ValueError):
        return None

       
if __name__ == "__main__":
    # Example usage
    image_path = "https://github.com/ianare/exif-samples/blob/master/jpg/gps/DSCN0010.jpg?raw=true"
    # image_path = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/new-business/IMG_8044.HEIC"
    print(f"Reading GPS data from {image_path}")
    try:
        lat, lon = get_gps_coordinates(image_path)
        print("data pull tried")
        if lat is not None and lon is not None:
            print(f"Latitude: {lat}, Longitude: {lon}")
        else:
            print("No GPS data found.")
    except Exception as e:
        print(f"Error occurred: {e}")
#     lat = 42.7643
#     lon = -71.03797777777777
#     resp = get_street_address_from_lat_lon(lat, lon)
#     print(resp)
    # full_address = "Treehouse Brewing, Deerfield, MA, USA"
    # business_name = "Barreled Souls Brewing"
    # city = "Saco"
    # state = "ME"
    # # resp = get_lat_lon("12 King Street, Groveland, MA, 01834, USA")
    # # print(resp["latitude"])
    # resp = get_street_address(business_name, city, state)
    # if resp:
    #     print(resp.address)
    # else:
    #     print("Address not found")
