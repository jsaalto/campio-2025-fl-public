import pyodbc
import os
import json
from fx_geo_utilities import get_lat_lon
from dotenv import load_dotenv
from datetime import datetime

def db_get_establishment_name(establishment: str):
    conn_str = db_get_connection_string()
    try:
        with pyodbc.connect(conn_str) as wifild_conn:
            gtvnu_cursor = wifild_conn.cursor()
            gtvnu_cursor.execute("SELECT establishment_name FROM Establishments.Establishment WHERE Establishment = ?", establishment)
            establishment_name = gtvnu_cursor.fetchone()
            wifild_conn.commit()
        if establishment_name:
            return establishment_name[0]
    except Exception as db_exc:
        print(f"Database error: {db_exc}")

def db_get_venue_name(venue: str):
    conn_str = db_get_connection_string()
    try:
        with pyodbc.connect(conn_str) as wifild_conn:
            gtvnu_cursor = wifild_conn.cursor()
            gtvnu_cursor.execute("SELECT venue_name FROM Establishments.Venue WHERE venue = ?", venue)
            venue_name = gtvnu_cursor.fetchone()
            wifild_conn.commit()
        if venue_name:
            return venue_name[0]
    except Exception as db_exc:
        print(f"Database error: {db_exc}")

def db_get_homepage_process_list():
    conn_str = db_get_connection_string()
    process_list = []
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""EXEC [Establishments].[get_homepage_process_list]""")
            for row in cursor.fetchall():
                cleaned_row = [str(item).strip('",()') for item in row]
                process_list.append(cleaned_row)
            conn.commit()
    except pyodbc.Error as e:
        print(f"Database error: {e}")
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
    return process_list

def db_establishment_upsert(establishment: dict) -> dict:
    establishment_code = establishment.get("establishment")
    establishment_name = establishment.get("establishment_name", "")
    is_active = 1
    create_datetime = datetime.now()
    created_by = "system"
    modified_datetime = datetime.now()
    modified_by = "system"
    try:
        conn_str = db_get_connection_string()
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            # Build the base SQL for venue and content
            cursor.execute(""" IF NOT EXISTS (SELECT 1 FROM [Establishments].[Establishment] WHERE Establishment = ?)
                BEGIN
                    INSERT INTO [Establishments].[Establishment]
                        ([Establishment]
                        ,[Establishment_Name]
                        ,[is_active]
                        ,[Create_Date]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                END""", establishment_code, establishment_code, establishment_name, is_active, create_datetime, created_by, modified_datetime, modified_by)
    except Exception as e:
        print(f"Error occurred: {e}")
    return "Establishment added to Database"

def db_venue_upsert(venue: dict) -> dict:
    address_line_1 = venue.get("address_line_1", "")
    address_line_2 = venue.get("address_line_2", "")
    city = venue.get("city", "")
    state = venue.get("state", "")
    postal_code = venue.get("postal_code", "")
    if postal_code:
        address_string = f"{address_line_1}, {address_line_2}, {city}, {state}, {postal_code}, USA"
    else:
        address_string = f"{address_line_1}, {address_line_2}, {city}, {state}, USA"
    location_info = get_lat_lon(address_string)
    latitude = location_info.get("latitude")
    longitude = location_info.get("longitude")
    name = venue.get("name", "")
    venue_url = venue.get("venue_url", "")
    homepage_url = venue.get("homepage_url", "")
    homepage_url = homepage_url.replace('https://', '').replace('http://', '').rstrip('/')
    venue_beer_list_url = venue.get("venue_beer_list_url", "")
    venue_beer_list_url = venue_beer_list_url.replace('https://', '').replace('http://', '').rstrip('/')
    vbl_content = venue_beer_list_url + '#web'
    venue_url = venue.get('venue_url')
    if not venue_url or not isinstance(venue_url, str):
        return {"message": "'venue_url' must be provided as a string", "status_code": 400}
    trimmed_venue_url = venue_url.replace('https://', '').replace('http://', '').rstrip('/')
    homepage_url = venue.get('homepage_url', '')
    content = trimmed_venue_url + '#web'
    venue_id = 'VNU#' + trimmed_venue_url + '#web'
    venue_name = venue.get('name', '')
    country_code = 'USA'
    venue_to_content = 'VNUCNTNT#' + trimmed_venue_url + '#VENUEHOMEPAGE#' + trimmed_venue_url + '#web'
    venue_to_content_type = 'Venue Homepage'
    venue_establishment = homepage_url.replace('https://', '').replace('http://', '').rstrip('/') + '#web'
    content_type = "Venue Homepage"
    content_group = "Homepage"
    content_category = "Website"
    content_url = trimmed_venue_url
    vbl_content_type = "Venue Beer List"
    vbl_content_group = "Product List Page"
    vbl_content_category = "Website"
    vbl_venue_to_content = 'VNUCNTNT#' + trimmed_venue_url + '#VENUEBEERLIST#' + venue_beer_list_url + '#web'
    vbl_venue_to_content_type = 'Venue Beer List'
    is_active = True
    is_validated = True
    last_scraped_datetime = datetime.now()
    create_datetime = datetime.now()
    created_by = "system"
    modified_datetime = None
    modified_by = None

    try:
        conn_str = db_get_connection_string()
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            # Build the base SQL for venue and content
            base_sql = """
                IF NOT EXISTS (SELECT 1 FROM [Establishments].[Venue] WHERE Venue = ?)
                BEGIN
                    INSERT INTO [Establishments].[Venue]
                    ([Venue]
                    ,[Venue_Establishment]
                    ,[Venue_Name]
                    ,[Address_Line_1]
                    ,[Address_Line_2]
                    ,[City]
                    ,[State_or_Province_Code]
                    ,[Country_Code]
                    ,[Postal_Code]
                    ,[Latitude]
                    ,[Longitude]
                    ,[is_active]
                    ,[Create_Datetime]
                    ,[Created_By]
                    ,[Modified_Datetime]
                    ,[Modified_By])                
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
                IF NOT EXISTS (SELECT 1 FROM [Content].[Content] WHERE Content = ?)
                BEGIN
                    INSERT INTO [Content].[Content]
                        ([Content]
                        ,[Content_Type]
                        ,[Content_Group]
                        ,[Content_Category]
                        ,[Content_URL]
                        ,[Is_Active]
                        ,[Last_Scraped_Datetime]
                        ,[Create_Date]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
                IF NOT EXISTS (SELECT 1 FROM [Establishments].[Venue_To_Content] WHERE Venue_To_Content = ?)
                BEGIN
                    INSERT INTO [Establishments].[Venue_To_Content]
                    ([Venue_To_Content]
                    ,[Venue_To_Content_Type]
                    ,[Venue]
                    ,[Content]
                    ,[Is_Validated]
                    ,[Is_Active]
                    ,[Create_Datetime]
                    ,[Created_By]
                    ,[Modified_Datetime]
                    ,[Modified_By])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
            """
            
            # Build base parameters
            base_params = [
                venue_id,
                venue_id, venue_establishment, venue_name, address_line_1, address_line_2, city, state, country_code, postal_code, latitude, longitude, is_active, create_datetime, created_by, modified_datetime, modified_by,
                content,
                content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_datetime, created_by, modified_datetime, modified_by,
                venue_to_content,
                venue_to_content, venue_to_content_type, venue_id, content, is_validated, is_active, create_datetime, created_by, modified_datetime, modified_by
            ]
            
            # Add beer list content if venue_beer_list_url is provided
            if venue_beer_list_url:
                vbl_sql = """
                    IF NOT EXISTS (SELECT 1 FROM [Content].[Content] WHERE Content = ?)
                    BEGIN
                        INSERT INTO [Content].[Content]
                            ([Content]
                            ,[Content_Type]
                            ,[Content_Group]
                            ,[Content_Category]
                            ,[Content_URL]
                            ,[Is_Active]
                            ,[Last_Scraped_Datetime]
                            ,[Create_Date]
                            ,[Created_By]
                            ,[Modified_Datetime]
                            ,[Modified_By])
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    END
                    IF NOT EXISTS (SELECT 1 FROM [Establishments].[Venue_To_Content] WHERE Venue_To_Content = ?)
                    BEGIN
                        INSERT INTO [Establishments].[Venue_To_Content]
                        ([Venue_To_Content]
                        ,[Venue_To_Content_Type]
                        ,[Venue]
                        ,[Content]
                        ,[Is_Validated]
                        ,[Is_Active]
                        ,[Create_Datetime]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    END
                """
                final_sql = base_sql + vbl_sql
                vbl_params = [
                    vbl_content,
                    vbl_content, vbl_content_type, vbl_content_group, vbl_content_category, venue_beer_list_url, is_active, last_scraped_datetime, create_datetime, created_by, modified_datetime, modified_by,
                    vbl_venue_to_content,
                    vbl_venue_to_content, vbl_venue_to_content_type, venue_id, vbl_content, is_validated, is_active, create_datetime, created_by, modified_datetime, modified_by
                ]
                all_params = base_params + vbl_params
            else:
                final_sql = base_sql
                all_params = base_params
            
            cursor.execute(final_sql, *all_params)
            
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}



def db_venue_page_upsert(venues: list[dict]) -> list[dict]:
    """
    Upserts venue pages using a list of dictionaries, each with 'venue_url' and 'homepage_url' keys.
    Returns a list of result dictionaries for each upsert.
    """
    if not isinstance(venues, list):
        raise ValueError("venues must be a list of dictionaries")
    results = []
    for venue_dict in venues:
        if not isinstance(venue_dict, dict):
            results.append({"message": "Each item must be a dictionary", "status_code": 400})
            continue
        venue_url = venue_dict.get('venue_url')
        homepage_url = venue_dict.get('homepage_url')
        if not isinstance(venue_url, str) or not isinstance(homepage_url, str):
            results.append({"message": "Both 'venue_url' and 'homepage_url' must be strings", "status_code": 400})
            continue

        # If venue_url starts with '/', prepend homepage_url
        if venue_url.startswith('/'):
            venue_url = homepage_url.rstrip('/') + venue_url
        trimmed_venue_url = venue_url.replace('https://', '').replace('http://', '')

        conn_str = db_get_connection_string()
        content = trimmed_venue_url + '#web'
        venue = 'VNU#' + trimmed_venue_url + '#web'
        venue_to_content = 'VNU#' + trimmed_venue_url + '#' + venue_url + '#web'
        venue_to_content_type = 'Venue Homepage'
        content_type = "Venue Homepage"
        content_group = "Homepage"
        content_category = "Website"
        content_url = trimmed_venue_url
        is_active = True
        is_validated = True
        last_scraped_datetime = datetime.now()
        create_datetime = datetime.now()
        created_by = "system"
        modified_datetime = None
        modified_by = None

        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM [Content].[Content] WHERE Content = ?)
                    BEGIN
                        INSERT INTO [Content].[Content]
                            ([Content]
                            ,[Content_Type]
                            ,[Content_Group]
                            ,[Content_Category]
                            ,[Content_URL]
                            ,[Is_Active]
                            ,[Last_Scraped_Datetime]
                            ,[Create_Date]
                            ,[Created_By]
                            ,[Modified_Datetime]
                            ,[Modified_By])
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    END
                    IF NOT EXISTS (SELECT 1 FROM [Establishments].[Venue_To_Content] WHERE Venue_To_Content = ?)
                    BEGIN
                        INSERT INTO [Establishments].[Venue_To_Content]
                        ([Venue_To_Content]
                        ,[Venue_To_Content_Type]
                        ,[Venue]
                        ,[Content]
                        ,[Is_Validated]
                        ,[Is_Active]
                        ,[Create_Datetime]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    END
                """,
                content,
                content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_datetime, created_by, modified_datetime, modified_by,
                venue_to_content,
                venue_to_content, venue_to_content_type, venue, content, is_validated, is_active, create_datetime, created_by, modified_datetime, modified_by)
                conn.commit()
                results.append({"message": "Upsert operation completed successfully.", "status_code": 200})
        except pyodbc.Error as e:
            print(f"Database error: {e}")
            results.append({"message": f"Database error: {e}", "status_code": 500})
        except Exception as db_exc:
            print(f"Database error: {db_exc}")
            results.append({"message": f"Database error: {db_exc}", "status_code": 500})
    return results


def db_mastodon_page_upsert(mastodon_url, homepage_url):
    conn_str = db_get_connection_string()

    content = mastodon_url + '#web'
    establishment = homepage_url + '#web'
    establishment_to_content = homepage_url + '#MASTODONHOMEPAGE#' + mastodon_url + '#web'
    content_type = "Mastodon Homepage"
    establishment_to_content_type = "Mastodon Homepage"
    content_group = "Homepage"
    content_category = "Website"
    content_url = mastodon_url
    is_active = True
    is_validated = True
    last_scraped_datetime = datetime.now()
    create_datetime = datetime.now()
    created_by = "system"
    modified_datetime = None
    modified_by = None

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM [Content].[Content] WHERE Content = ?)
                BEGIN
                    INSERT INTO [Content].[Content]
                        ([Content]
                        ,[Content_Type]
                        ,[Content_Group]
                        ,[Content_Category]
                        ,[Content_URL]
                        ,[Is_Active]
                        ,[Last_Scraped_Datetime]
                        ,[Create_Date]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
                IF NOT EXISTS (SELECT 1 FROM [Establishments].[Establishment_To_Content] WHERE Establishment_To_Content = ?)
                BEGIN
                    INSERT INTO [Establishments].[Establishment_To_Content]
                    ([Establishment_To_Content]
                    ,[Establishment_To_Content_Type]
                    ,[Establishment]
                    ,[Content]
                    ,[Is_Validated]
                    ,[Is_Active]
                    ,[Create_Datetime]
                    ,[Created_By]
                    ,[Modified_Datetime]
                    ,[Modified_By])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
            """
            , content
            , content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_datetime, created_by, modified_datetime, modified_by
            , establishment_to_content
            , establishment_to_content, establishment_to_content_type, establishment, content, is_validated, is_active, create_datetime, created_by, modified_datetime, modified_by)
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}

def db_bluesky_page_upsert(bluesky_url, homepage_url):
    conn_str = db_get_connection_string()

    content = bluesky_url + '#web'
    establishment = homepage_url + '#web'
    establishment_to_content = homepage_url + '#BLUESKYHOMEPAGE#' + bluesky_url + '#web'
    content_type = "Bluesky Homepage"
    establishment_to_content_type = "Bluesky Homepage"
    content_group = "Homepage"
    content_category = "Website"
    content_url = bluesky_url
    is_active = True
    is_validated = True
    last_scraped_datetime = datetime.now()
    create_datetime = datetime.now()
    created_by = "system"
    modified_datetime = None
    modified_by = None

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM [Content].[Content] WHERE Content = ?)
                BEGIN
                    INSERT INTO [Content].[Content]
                        ([Content]
                        ,[Content_Type]
                        ,[Content_Group]
                        ,[Content_Category]
                        ,[Content_URL]
                        ,[Is_Active]
                        ,[Last_Scraped_Datetime]
                        ,[Create_Date]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
                IF NOT EXISTS (SELECT 1 FROM [Establishments].[Establishment_To_Content] WHERE Establishment_To_Content = ?)
                BEGIN
                    INSERT INTO [Establishments].[Establishment_To_Content]
                    ([Establishment_To_Content]
                    ,[Establishment_To_Content_Type]
                    ,[Establishment]
                    ,[Content]
                    ,[Is_Validated]
                    ,[Is_Active]
                    ,[Create_Datetime]
                    ,[Created_By]
                    ,[Modified_Datetime]
                    ,[Modified_By])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
            """
            , content
            , content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_datetime, created_by, modified_datetime, modified_by
            , establishment_to_content
            , establishment_to_content, establishment_to_content_type, establishment, content, is_validated, is_active, create_datetime, created_by, modified_datetime, modified_by)
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}


def db_x_page_upsert(x_url, homepage_url):
    conn_str = db_get_connection_string()

    content = x_url + '#web'
    establishment = homepage_url + '#web'
    establishment_to_content = homepage_url + '#XHOMEPAGE#' + x_url + '#web'
    content_type = "X (Twitter) Homepage"
    establishment_to_content_type = "X (Twitter) Homepage"
    content_group = "Homepage"
    content_category = "Website"
    content_url = x_url
    is_active = True
    is_validated = True
    last_scraped_datetime = datetime.now()
    create_datetime = datetime.now()
    created_by = "system"
    modified_datetime = None
    modified_by = None

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM [Content].[Content] WHERE Content = ?)
                BEGIN
                    INSERT INTO [Content].[Content]
                        ([Content]
                        ,[Content_Type]
                        ,[Content_Group]
                        ,[Content_Category]
                        ,[Content_URL]
                        ,[Is_Active]
                        ,[Last_Scraped_Datetime]
                        ,[Create_Date]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
                IF NOT EXISTS (SELECT 1 FROM [Establishments].[Establishment_To_Content] WHERE Establishment_To_Content = ?)
                BEGIN
                    INSERT INTO [Establishments].[Establishment_To_Content]
                    ([Establishment_To_Content]
                    ,[Establishment_To_Content_Type]
                    ,[Establishment]
                    ,[Content]
                    ,[Is_Validated]
                    ,[Is_Active]
                    ,[Create_Datetime]
                    ,[Created_By]
                    ,[Modified_Datetime]
                    ,[Modified_By])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
            """
            , content
            , content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_datetime, created_by, modified_datetime, modified_by
            , establishment_to_content
            , establishment_to_content, establishment_to_content_type, establishment, content, is_validated, is_active, create_datetime, created_by, modified_datetime, modified_by)
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}


def db_instagram_page_upsert(instagram_url, homepage_url):
    conn_str = db_get_connection_string()

    content = instagram_url + '#web'
    establishment = homepage_url + '#web'
    establishment_to_content = homepage_url + '#INSTAGRAMHOMEPAGE#' + instagram_url + '#web'
    establishment_to_content_type = "Instagram Homepage"
    content_type = "Instagram Homepage"
    content_group = "Homepage"
    content_category = "Website"
    content_url = instagram_url
    is_active = True
    is_validated = True
    last_scraped_datetime = datetime.now()
    create_datetime = datetime.now()
    created_by = "system"
    modified_datetime = None
    modified_by = None

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM [Content].[Content] WHERE Content = ?)
                BEGIN
                    INSERT INTO [Content].[Content]
                        ([Content]
                        ,[Content_Type]
                        ,[Content_Group]
                        ,[Content_Category]
                        ,[Content_URL]
                        ,[Is_Active]
                        ,[Last_Scraped_Datetime]
                        ,[Create_Date]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
                IF NOT EXISTS (SELECT 1 FROM [Establishments].[Establishment_To_Content] WHERE Establishment_To_Content = ?)
                BEGIN
                    INSERT INTO [Establishments].[Establishment_To_Content]
                    ([Establishment_To_Content]
                    ,[Establishment_To_Content_Type]
                    ,[Establishment]
                    ,[Content]
                    ,[Is_Validated]
                    ,[Is_Active]
                    ,[Create_Datetime]
                    ,[Created_By]
                    ,[Modified_Datetime]
                    ,[Modified_By])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
            """
            , content
            , content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_datetime, created_by, modified_datetime, modified_by
            , establishment_to_content
            , establishment_to_content, establishment_to_content_type, establishment, content, is_validated, is_active, create_datetime, created_by, modified_datetime, modified_by)
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}

def db_facebook_business_page_upsert(facebook_url, homepage_url):
    conn_str = db_get_connection_string()

    content = facebook_url + '#web'
    establishment = homepage_url + '#web'
    establishment_to_content = homepage_url + '#FACEBOOKPAGE#' + facebook_url + '#web'
    establishment_to_content_type = "Facebook Homepage"
    content_type = "Facebook Homepage"
    content_group = "Homepage"
    content_category = "Website"
    content_url = facebook_url
    is_active = True
    is_validated = True
    last_scraped_datetime = datetime.now()
    create_datetime = datetime.now()
    created_by = "system"
    modified_datetime = None
    modified_by = None

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM [Content].[Content] WHERE Content = ?)
                BEGIN
                    INSERT INTO [Content].[Content]
                        ([Content]
                        ,[Content_Type]
                        ,[Content_Group]
                        ,[Content_Category]
                        ,[Content_URL]
                        ,[Is_Active]
                        ,[Last_Scraped_Datetime]
                        ,[Create_Date]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
                IF NOT EXISTS (SELECT 1 FROM [Establishments].[Establishment_To_Content] WHERE Establishment_To_Content = ?)
                BEGIN
                    INSERT INTO [Establishments].[Establishment_To_Content]
                    ([Establishment_To_Content]
                    ,[Establishment_To_Content_Type]
                    ,[Establishment]
                    ,[Content]
                    ,[Is_Validated]
                    ,[Is_Active]
                    ,[Create_Datetime]
                    ,[Created_By]
                    ,[Modified_Datetime]
                    ,[Modified_By])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
            """
            , content
            , content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_datetime, created_by, modified_datetime, modified_by
            , establishment_to_content
            , establishment_to_content, establishment_to_content_type, establishment, content, is_validated, is_active, create_datetime, created_by, modified_datetime, modified_by)
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}

def db_logo_url_upsert(image_url, homepage_url):
    conn_str = db_get_connection_string()

    content = image_url + '#img'
    establishment = homepage_url + '#web'
    establishment_to_content = homepage_url + '#LOGOIMAGE#' + image_url + '#web'
    establishment_to_content_type = "Logo Image"
    content_type = "Logo Image"
    content_group = "Logo Image"
    content_category = "Image"
    content_url = image_url
    is_active = True
    is_validated = True
    last_scraped_datetime = datetime.now()
    create_datetime = datetime.now()
    created_by = "system"
    modified_datetime = None
    modified_by = None

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM [Content].[Content] WHERE Content = ?)
                BEGIN
                    INSERT INTO [Content].[Content]
                        ([Content]
                        ,[Content_Type]
                        ,[Content_Group]
                        ,[Content_Category]
                        ,[Content_URL]
                        ,[Is_Active]
                        ,[Last_Scraped_Datetime]
                        ,[Create_Date]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
                IF NOT EXISTS (SELECT 1 FROM [Establishments].[Establishment_To_Content] WHERE Establishment_To_Content = ?)
                BEGIN
                    INSERT INTO [Establishments].[Establishment_To_Content]
                    ([Establishment_To_Content]
                    ,[Establishment_To_Content_Type]
                    ,[Establishment]
                    ,[Content]
                    ,[Is_Validated]
                    ,[Is_Active]
                    ,[Create_Datetime]
                    ,[Created_By]
                    ,[Modified_Datetime]
                    ,[Modified_By])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
            """
            , content
            , content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_datetime, created_by, modified_datetime, modified_by
            , establishment_to_content
            , establishment_to_content, establishment_to_content_type, establishment, content, is_validated, is_active, create_datetime, created_by, modified_datetime, modified_by)
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}



def db_get_connection_string():
    server = 'fl-campio-2025-svr.database.windows.net'
    database = 'campio-2025-fl-db'
    driver = '{ODBC Driver 17 for SQL Server}'
    # Load environment variables from .env file
    load_dotenv()
    pwd = os.getenv('SQL_PASSWORD')
    username = os.getenv('SQL_USERNAME')
 
    # Build connection string for Azure SQL with Managed Identity access
    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        # "Authentication=ActiveDirectoryMsi;"
        f"UID={username};"
        f"PWD={pwd};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=60;"
        "LoginTimeout=60;"
    )
    return conn_str

# if __name__ == '__main__':
#     resp = db_get_homepage_process_list()
#     print(resp)
#     resp = db_upsert_wifi_network_password(venue, WiFi_Network, WiFi_Password, True, True, "MCP Server")


