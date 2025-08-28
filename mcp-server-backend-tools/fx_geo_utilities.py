# importing geopy library and Nominatim class
import requests
from geopy.geocoders import Nominatim

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
    print(getLoc.address)

    return "Success"

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


# if __name__ == "__main__":
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
