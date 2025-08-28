import pyodbc
import os
import json
from fx_utilities import get_street_address_from_lat_lon
from dotenv import load_dotenv
from datetime import datetime

def db_upsert_wifi_network_password(Venue: str, Wifi_Network: str, Wifi_Password: str, is_primary: bool, is_active: bool, Process_user: str):
    conn_str = db_get_connection_string()
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""EXEC [Establishments].[upsert_Venue_Wifi_Password] ?, ?, ?, ?, ?, ?"""
                , Venue, Wifi_Network, Wifi_Password, is_primary, is_active, Process_user)
            conn.commit()
            print(f"WiFi Network Password Upserted for Venue: {Venue}")
    except pyodbc.Error as e:
        print(f"Database error: {e}")
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
    return "WiFi Network Password Upserted to Database Successfully"

def db_create_new_business_from_image(category: str, latitude: float, longitude: float, full_address_string: str, image_url: str, business_name: str = None):
    conn_str = db_get_connection_string()
    if conn_str is None:
        return {"error": "Database connection string is not available"}
    address_from_lat_lon = get_street_address_from_lat_lon(latitude, longitude)
    if address_from_lat_lon is None:
        return {"error": "Unable to retrieve address from latitude/longitude"}
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""EXEC [mbl].[new_business_create_off_image] ?, ?, ?, ?, ?, ?""",
                           category, latitude, longitude, full_address_string, image_url, business_name)
            rows = cursor.fetchall()
            print(rows)
            return [{"venue": row.Venue} for row in rows]
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"error": f"Database error: {e}"}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"error": f"Database error: {db_exc}"}

def db_get_business_list_by_lat_lon(category: str, latitude: float, longitude: float, radius_miles: float, return_count: int):
    conn_str = db_get_connection_string()
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""EXEC [mbl].[business_list_by_lat_lon] ?, ?, ?, ?, ?""",
                category, latitude, longitude, radius_miles, return_count)
            rows = cursor.fetchall()
            return [{"venue": row.venue, "venue_name": row.venue_name, "full_address": row.full_address, "distance": row.distance, "image_url": row.image_url} for row in rows]
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"error": f"Database error: {e}"}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"error": f"Database error: {db_exc}"}

def db_get_business_types():
    conn_str = db_get_connection_string()
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""EXEC [mbl].[business_types] """)
            rows = cursor.fetchall()
            return [{"business_type": row.Tag_Category} for row in rows]
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"error": f"Database error: {e}"}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"error": f"Database error: {db_exc}"}

def db_image_to_establishment_content_upsert(content, content_type, content_group, content_category
                                     , content_url, is_active, last_scraped_datetime, create_date
                                     , created_by, modified_datetime, modified_by, establishment_to_content
                                     , establishment):
    conn_str = db_get_connection_string()
    try:
        with pyodbc.connect(conn_str) as conn:
            estcursor = conn.cursor()
            estcursor.execute("""
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
                    ,[Establishment]
                    ,[Content])
                    VALUES (?, ?, ?)        
                END
            """, content, content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_date, created_by, modified_datetime, modified_by
                , establishment_to_content, establishment_to_content, establishment, content)
            conn.commit()
            print(f"Content upserted for establishment: {establishment_to_content}")
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}

def db_image_to_venue_content_upsert(content, content_type, content_group, content_category
                                     , content_url, is_active, is_validated, last_scraped_datetime, create_date
                                     , created_by, modified_datetime, modified_by, venue_to_content
                                     , venue, venue_to_content_type):
    conn_str = db_get_connection_string()
    try:
        with pyodbc.connect(conn_str) as conn:
            estcursor = conn.cursor()
            estcursor.execute("""
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
                        ,[Is_Active]
                        ,[Is_Validated]
                        ,[Create_Datetime]
                        ,[Created_By]
                        ,[Modified_Datetime]
                        ,[Modified_By])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)        
                    END
                """, content, content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_date, created_by, modified_datetime, modified_by
                    , venue_to_content, venue_to_content, venue_to_content_type, venue, content, is_active, is_validated, create_date, created_by, modified_datetime, modified_by)
            conn.commit()
            print(f"Content upserted for venue: {venue_to_content}")
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}

def db_image_to_content_upsert(image_url, image_type, target_entity_link_type, homepage_url):
    content = image_url + '#web'
    if target_entity_link_type == "Establishment":
        establishment = homepage_url + '#web'
        establishment_to_content = homepage_url + '#' + image_url + '#img'
    elif target_entity_link_type == "Venue":
        venue = 'VNU#' + homepage_url + '#web'
        venue_to_content = homepage_url + '#' + image_url + '#img'
    if image_type == "WiFi":
        content_type = image_type
        content_group = "Cellphone Image"
        content_category = "Image"
        venue_to_content_type = image_type
    elif image_type == "New Business":
        content_type = "Business Image"
        content_group = "Cellphone Image"
        content_category = "Image"
        venue_to_content_type = "Business Image"
    elif image_type == "Business Logo":
        content_type = "Business Logo"
        content_group = "Web Image"
        content_category = "Image"
        venue_to_content_type = "Business Logo"
    content_url = image_url
    is_active = True
    is_validated = True
    last_scraped_datetime = datetime.now()
    create_date = datetime.now()
    created_by = "MCP Server"
    modified_datetime = None
    modified_by = None
    # print(f"Variables set: content_type: {content_type}, content_group: {content_group}, content_category: {content_category}, content_url: {content_url}, is_active: {is_active}, last_scraped_datetime: {last_scraped_datetime}, create_date: {create_date}, created_by: {created_by}, modified_datetime: {modified_datetime}, modified_by: {modified_by}")
    if target_entity_link_type == 'Establishment':
        db_response = db_image_to_establishment_content_upsert(content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_date, created_by, modified_datetime, modified_by, establishment_to_content, establishment)
        if db_response is None:
            return "Database error: Upsert failed for establishment"
    elif target_entity_link_type == 'Venue':
        db_response = db_image_to_venue_content_upsert(content, content_type, content_group, content_category, content_url, is_active, is_validated, last_scraped_datetime, create_date, created_by, modified_datetime, modified_by, venue_to_content, venue, venue_to_content_type) 
        if db_response is None:
            return "Database error: Upsert failed for venue"
    else:
        db_response = None
    if db_response is None:
        print("Upsert operation failed.")
    return db_response

def db_facebook_business_page_upsert(facebook_url, homepage_url):
    conn_str = db_get_connection_string()

    content = facebook_url + '#web'
    establishment = homepage_url + '#web'
    establishment_to_content = homepage_url + '#' + facebook_url + '#web'
    content_type = "Facebook Homepage"
    content_group = "Homepage"
    content_category = "Website"
    content_url = facebook_url
    is_active = True
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
                    ,[Establishment]
                    ,[Content]
                    ,[Is_Validated]
                    ,[Is_Active]
                    ,[Create_Datetime]
                    ,[Created_By]
                    ,[Modified_Datetime]
                    ,[Modified_By])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
            """, content, content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_datetime, created_by, modified_datetime, modified_by
                , establishment_to_content, establishment_to_content, establishment, content, is_active, create_datetime, created_by, modified_datetime, modified_by)
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}

def db_output_list_to_stage(output_list, analyzer_name):
    conn_str = db_get_connection_string()
    list_ct = 0
    if output_list:   
        list_ct = len(output_list)
        try:
            with pyodbc.connect(conn_str) as oltrunc_conn:
                oltrunc_cursor = oltrunc_conn.cursor()
                oltrunc_cursor.execute("TRUNCATE TABLE [Stage].[CU_Output_List]")
                oltrunc_conn.commit()
            print(f"Stage.CU_Output_List truncated.")
        except Exception as db_exc:
            print(f"Database error: {db_exc}")
            pass
        try:
            with pyodbc.connect(conn_str) as olload_conn:
                for item in output_list:
                    # Ensure item is a dictionary and has the required keys
                    if not isinstance(item, dict):
                        print(f"Warning: Item is not a dictionary: {type(item)} - {item}")
                        continue
                    
                    if 'raw_item_json' not in item or 'source' not in item or 'pull_datetime' not in item:
                        print(f"Warning: Item missing required keys: {item}")
                        continue
                    
                    current_datetime = datetime.now()
                    raw_item_json_str = json.dumps(item['raw_item_json']) if isinstance(item['raw_item_json'], dict) else item['raw_item_json']
                    olload_cursor = olload_conn.cursor()
                    olload_cursor.execute("INSERT INTO [Stage].[CU_Output_List] ([Raw_Item_Json],[Source],[Pull_Datetime],[Create_Date],[Created_By],[Modified_Datetime],[Modified_By]) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                          (raw_item_json_str, item['source'], item['pull_datetime'], current_datetime, analyzer_name, None, None))
                olload_conn.commit()
            print(f"Database load complete.")
        except Exception as db_exc:
            print(f"Database error: {db_exc}")
            pass
    else:
        print("No output list provided for load.")
        pass
    return f"Success. Loaded {list_ct} items to Stage.CU_Output_List."

def db_pull_homepage_list():
    conn_str = db_get_connection_string()

    try:
        with pyodbc.connect(conn_str) as phl_conn:
            phl_cursor = phl_conn.cursor()
            phl_cursor.execute("SELECT top 500 content_url FROM Content.Content WHERE Is_Active = 1 AND Content_Type = 'Homepage' AND ISNULL(Modified_datetime, '1900-01-01') < CONVERT(DATE, GETDATE())")
            hp_list = [row[0] for row in phl_cursor.fetchall()]
            print(f"Homepage List Pull Complete: {hp_list}")
            return {"Homepage List": hp_list
                    , "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {
            "message": f"Database error: {e}",
            "status_code": 500
        }
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {
            "message": f"Database error: {db_exc}",
            "status_code": 500
        }

def db_stage_hours_of_operation_data(session_uid: str, venue: str,
    monday_hours_summary: str = None, monday_open_time = None, monday_close_time = None,
    tuesday_hours_summary: str = None, tuesday_open_time = None, tuesday_close_time = None,
    wednesday_hours_summary: str = None, wednesday_open_time = None, wednesday_close_time = None,
    thursday_hours_summary: str = None, thursday_open_time = None, thursday_close_time = None,
    friday_hours_summary: str = None, friday_open_time = None, friday_close_time = None,
    saturday_hours_summary: str = None, saturday_open_time = None, saturday_close_time = None,
    sunday_hours_summary: str = None, sunday_open_time = None, sunday_close_time = None,
    schedule_effective_start_date = None, schedule_effective_end_date = None,
    content_url: str = None):
    print("Staging hours of operation data...")
    
    # If venue is a dict, extract the string value (adjust the key as needed)
    if isinstance(venue, dict):
        venue_str = venue.get('venue') or venue.get('venue_name') or next(iter(venue.values()), '')
    else:
        venue_str = venue
    
    # Handle schedule dates - check for None, empty dict, or missing values
    start_date_is_empty = (schedule_effective_start_date is None or 
                          (isinstance(schedule_effective_start_date, dict) and not schedule_effective_start_date) or
                          schedule_effective_start_date == '')
    end_date_is_empty = (schedule_effective_end_date is None or 
                        (isinstance(schedule_effective_end_date, dict) and not schedule_effective_end_date) or
                        schedule_effective_end_date == '')
    
    if start_date_is_empty and end_date_is_empty:
        hours_of_operation_type = "Base"
    else:
        hours_of_operation_type = "Temporary"

    if isinstance(session_uid, dict):
        if session_uid == {}:
            session_uid = None
        else:
            session_uid = session_uid.get('session_uid') or next(iter(session_uid.values()), '')
    if isinstance(monday_hours_summary, dict):
        if monday_hours_summary == {}:
            monday_hours_summary = None
        else:
            monday_hours_summary = monday_hours_summary.get('monday_hours_summary') or next(iter(monday_hours_summary.values()), '')
    if isinstance(monday_open_time, dict):
        if monday_open_time == {}:
            monday_open_time = None
        else:
            monday_open_time = monday_open_time.get('monday_open_time') or next(iter(monday_open_time.values()), '')
    if isinstance(monday_close_time, dict):
        if monday_close_time == {}:
            monday_close_time = None
        else:
            monday_close_time = monday_close_time.get('monday_close_time') or next(iter(monday_close_time.values()), '')
    if isinstance(tuesday_hours_summary, dict):
        if tuesday_hours_summary == {}:
            tuesday_hours_summary = None
        else:
            tuesday_hours_summary = tuesday_hours_summary.get('tuesday_hours_summary') or next(iter(tuesday_hours_summary.values()), '')
    if isinstance(tuesday_open_time, dict):
        if tuesday_open_time == {}:
            tuesday_open_time = None
        else:
            tuesday_open_time = tuesday_open_time.get('tuesday_open_time') or next(iter(tuesday_open_time.values()), '')
    if isinstance(tuesday_close_time, dict):
        if tuesday_close_time == {}:
            tuesday_close_time = None
        else:
            tuesday_close_time = tuesday_close_time.get('tuesday_close_time') or next(iter(tuesday_close_time.values()), '')
    if isinstance(wednesday_hours_summary, dict):
        if wednesday_hours_summary == {}:
            wednesday_hours_summary = None
        else:
            wednesday_hours_summary = wednesday_hours_summary.get('wednesday_hours_summary') or next(iter(wednesday_hours_summary.values()), '')
    if isinstance(wednesday_open_time, dict):
        if wednesday_open_time == {}:
            wednesday_open_time = None
        else:
            wednesday_open_time = wednesday_open_time.get('wednesday_open_time') or next(iter(wednesday_open_time.values()), '')
    if isinstance(wednesday_close_time, dict):
        if wednesday_close_time == {}:
            wednesday_close_time = None
        else:
            wednesday_close_time = wednesday_close_time.get('wednesday_close_time') or next(iter(wednesday_close_time.values()), '')
    if isinstance(thursday_hours_summary, dict):
        if thursday_hours_summary == {}:
            thursday_hours_summary = None
        else:
            thursday_hours_summary = thursday_hours_summary.get('thursday_hours_summary') or next(iter(thursday_hours_summary.values()), '')
    if isinstance(thursday_open_time, dict):
        if thursday_open_time == {}:
            thursday_open_time = None
        else:
            thursday_open_time = thursday_open_time.get('thursday_open_time') or next(iter(thursday_open_time.values()), '')
    if isinstance(thursday_close_time, dict):
        if thursday_close_time == {}:
            thursday_close_time = None
        else:
            thursday_close_time = thursday_close_time.get('thursday_close_time') or next(iter(thursday_close_time.values()), '')
    if isinstance(friday_hours_summary, dict):
        if friday_hours_summary == {}:
            friday_hours_summary = None
        else:
            friday_hours_summary = friday_hours_summary.get('friday_hours_summary') or next(iter(friday_hours_summary.values()), '')
    if isinstance(friday_open_time, dict):
        if friday_open_time == {}:
            friday_open_time = None
        else:
            friday_open_time = friday_open_time.get('friday_open_time') or next(iter(friday_open_time.values()), '')
    if isinstance(friday_close_time, dict):
        if friday_close_time == {}:
            friday_close_time = None
        else:
            friday_close_time = friday_close_time.get('friday_close_time') or next(iter(friday_close_time.values()), '')
    if isinstance(saturday_hours_summary, dict):
        if saturday_hours_summary == {}:
            saturday_hours_summary = None
        else:
            saturday_hours_summary = saturday_hours_summary.get('saturday_hours_summary') or next(iter(saturday_hours_summary.values()), '')
    if isinstance(saturday_open_time, dict):
        if saturday_open_time == {}:
            saturday_open_time = None
        else:
            saturday_open_time = saturday_open_time.get('saturday_open_time') or next(iter(saturday_open_time.values()), '')
    if isinstance(saturday_close_time, dict):
        if saturday_close_time == {}:
            saturday_close_time = None
        else:
            saturday_close_time = saturday_close_time.get('saturday_close_time') or next(iter(saturday_close_time.values()), '')
    if isinstance(sunday_hours_summary, dict):
        if sunday_hours_summary == {}:
            sunday_hours_summary = None
        else:
            sunday_hours_summary = sunday_hours_summary.get('sunday_hours_summary') or next(iter(sunday_hours_summary.values()), '')
    if isinstance(sunday_open_time, dict):
        if sunday_open_time == {}:
            sunday_open_time = None
        else:
            sunday_open_time = sunday_open_time.get('sunday_open_time') or next(iter(sunday_open_time.values()), '')
    if isinstance(sunday_close_time, dict):
        if sunday_close_time == {}:
            sunday_close_time = None
        else:
            sunday_close_time = sunday_close_time.get('sunday_close_time') or next(iter(sunday_close_time.values()), '')
    if isinstance(schedule_effective_start_date, dict):
        if schedule_effective_start_date == {}:
            schedule_effective_start_date = None
        else:
            schedule_effective_start_date = schedule_effective_start_date.get('schedule_effective_start_date') or next(iter(schedule_effective_start_date.values()), '')
    if isinstance(schedule_effective_end_date, dict):
        if schedule_effective_end_date == {}:
            schedule_effective_end_date = None
        else:
            schedule_effective_end_date = schedule_effective_end_date.get('schedule_effective_end_date') or next(iter(schedule_effective_end_date.values()), '')    

    if hours_of_operation_type == "Base":
        Venue_hours_of_Operation = "BASE#" + venue_str
    else:  # Temporary
        if schedule_effective_start_date:
            if isinstance(schedule_effective_start_date, str):
                try:
                    # Try to parse string to datetime
                    schedule_effective_start_date = datetime.strptime(schedule_effective_start_date, '%Y-%m-%d')
                except ValueError:
                    try:
                        # Try alternative date format
                        schedule_effective_start_date = datetime.strptime(schedule_effective_start_date, '%m/%d/%Y')
                    except ValueError:
                        # If parsing fails, use empty string
                        start_date_formatted = ''
                        schedule_effective_start_date = None
            
            if schedule_effective_start_date:
                start_date_formatted = schedule_effective_start_date.strftime('%Y%m%d')
            else:
                start_date_formatted = ''
        else:
            start_date_formatted = ''
        Venue_hours_of_Operation = "TEMP#" + start_date_formatted + "#" + venue_str
    gnrtd_datetime = datetime.now()

    conn_str = db_get_connection_string()

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO [Stage].[Mobile_Venue_Hours_of_Operation_Document]
                ([session_uid]
                ,[Venue_hours_of_Operation]
                ,[Venue]
                ,[Hours_of_Operation_Type]
                ,[monday_hours_summary]
                ,[monday_open_time]
                ,[monday_close_time]
                ,[tuesday_hours_summary]
                ,[tuesday_open_time]
                ,[tuesday_close_time]
                ,[wednesday_hours_summary]
                ,[wednesday_open_time]
                ,[wednesday_close_time]
                ,[thursday_hours_summary]
                ,[thursday_open_time]
                ,[thursday_close_time]
                ,[friday_hours_summary]
                ,[friday_open_time]
                ,[friday_close_time]
                ,[saturday_hours_summary]
                ,[saturday_open_time]
                ,[saturday_close_time]
                ,[sunday_hours_summary]
                ,[sunday_open_time]
                ,[sunday_close_time]
                ,[schedule_effective_start_date]
                ,[schedule_effective_end_date]
                ,[content_url]
                ,[stage_datetime])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, session_uid, Venue_hours_of_Operation, venue_str, hours_of_operation_type
                , monday_hours_summary, monday_open_time, monday_close_time
                , tuesday_hours_summary, tuesday_open_time, tuesday_close_time
                , wednesday_hours_summary, wednesday_open_time, wednesday_close_time
                , thursday_hours_summary, thursday_open_time, thursday_close_time
                , friday_hours_summary, friday_open_time, friday_close_time
                , saturday_hours_summary, saturday_open_time, saturday_close_time
                , sunday_hours_summary, sunday_open_time, sunday_close_time
                , schedule_effective_start_date, schedule_effective_end_date
                , content_url, gnrtd_datetime)
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}

def db_stage_wifi_data(session_uid: str, venue, wifi_network: str = None, wifi_password: str = None, content_url: str = None):
    print("Staging WiFi data...")
    conn_str = db_get_connection_string()

    # If venue is a dict, extract the string value (adjust the key as needed)
    if isinstance(venue, dict):
        venue_str = venue.get('venue') or venue.get('venue_name') or next(iter(venue.values()), '')
    else:
        venue_str = venue

    venue_wifi = venue_str.replace("VNU#", "") + "#" + (wifi_network.replace(" ", "") if wifi_network else "") + "#ALL"
    gnrtd_datetime = datetime.now()
    created_by = "system"
    modified_datetime = None
    modified_by = None

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO [Stage].[Mobile_Venue_Wifi_Image]
                                ([session_uid]
                                ,[Venue_Wifi]
                                ,[Venue]
                                ,[Wifi_Network]
                                ,[Wifi_password]
                                ,[content_url]
                                ,[stage_datetime])
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, session_uid, venue_wifi, venue, wifi_network, wifi_password, content_url, gnrtd_datetime)
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}

def db_stage_tap_list_data():
    print("Staging tap list data...")

def db_stage_product_offering_data(session_uid: str, is_new_business: bool, business_name: str, business_type: str,
                                    latitude: float, longitude: float, full_address_string: str, venue: str,
                                    product_list: list, content_url: str, stage_datetime: datetime):
    print("Staging product offering data...")
    conn_str = db_get_connection_string()

    # If venue is a dict, extract the string value (adjust the key as needed)
    if isinstance(venue, dict):
        venue_str = venue.get('venue') or venue.get('venue_name') or next(iter(venue.values()), '')
    else:
        venue_str = venue
    if isinstance(is_new_business, dict):
        is_new_business = is_new_business.get('is_new_business', False)
    if isinstance(business_name, dict):
        business_name = business_name.get('business_name', '')
    if isinstance(business_type, dict):
        business_type = business_type.get('business_type', '')
    if isinstance(latitude, dict):
        latitude = latitude.get('latitude', 0.0)
    if isinstance(longitude, dict):
        longitude = longitude.get('longitude', 0.0)
    if isinstance(full_address_string, dict):
        full_address_string = full_address_string.get('full_address_string', '')
    if isinstance(product_list, dict):
        product_list = product_list.get('product_list', '')
    if isinstance(content_url, dict):
        content_url = content_url.get('content_url', '')
    if isinstance(stage_datetime, dict):
        stage_datetime = stage_datetime.get('stage_datetime', datetime.now())

    gnrtd_datetime = datetime.now()
    created_by = "system"
    modified_datetime = None
    modified_by = None

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO [Stage].[Mobile_Venue_Product_Offerings_Document]
                    ([session_uid]
                    ,[is_new_business]
                    ,[business_name]
                    ,[business_type]
                    ,[latitude]
                    ,[longitude]
                    ,[full_address_string]
                    ,[venue]
                    ,[product_list]
                    ,[content_url]
                    ,[stage_datetime])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, session_uid, is_new_business, business_name, business_type, latitude, longitude, full_address_string, venue, product_list, content_url, stage_datetime)
            conn.commit()
            return {"message": "Upsert operation completed successfully.", "status_code": 200}
    except pyodbc.Error as e:
        print(f"Database error: {e}")
        return {"message": f"Database error: {e}", "status_code": 500}
    except Exception as db_exc:
        print(f"Database error: {db_exc}")
        return {"message": f"Database error: {db_exc}", "status_code": 500}

def db_load_stage_to_wifi(session_uid: str):
    conn_str = db_get_connection_string()
    try:
        with pyodbc.connect(conn_str) as wifild_conn:
            wifild_cursor = wifild_conn.cursor()
            wifild_cursor.execute("EXEC [Establishments].[mbl_upsert_Venue_Wifi_Password] ?", session_uid)
            wifild_conn.commit()
        print(f"Wifi Data loaded for session: {session_uid}")
        return f"Success. Wifi Data loaded for session: {session_uid}"
    except Exception as db_exc:
        print(f"Database error: {db_exc}")

def db_load_stage_to_hours_of_operation(session_uid: str):
    conn_str = db_get_connection_string()
    try:
        with pyodbc.connect(conn_str) as wifild_conn:
            wifild_cursor = wifild_conn.cursor()
            wifild_cursor.execute("EXEC [Establishments].[mbl_upsert_Venue_Hours_Of_Operation] ?", session_uid)
            wifild_conn.commit()
        print(f"Hours of Operation Data loaded for session: {session_uid}")
        return f"Success. Hours of Operation Data loaded for session: {session_uid}"
    except Exception as db_exc:
        print(f"Database error: {db_exc}")

def db_load_stage_to_product_offering(session_uid: str):
    conn_str = db_get_connection_string()
    try:
        with pyodbc.connect(conn_str) as pold_conn:
            pold_cursor = pold_conn.cursor()
            pold_cursor.execute("EXEC [Establishments].[mbl_upsert_Venue_Product_Offerings] ?", session_uid)
            pold_conn.commit()
        print(f"Product Offerings Data loaded for session: {session_uid}")
        return f"Success. Product Offerings Data loaded for session: {session_uid}"
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

if __name__ == '__main__':
    
    category = "Honey Stand"
    latitude = 42.7643
    longitude = -71.03797777777777
    full_address_string = "Lincoln Avenue, Haverhill, Essex County, Massachusetts, 01834, United States"
    image_url = "https://campio2025flmobile.blob.core.windows.net/directory-web-pages/Honey-stand-1.jpg"
    business_name = None

    print("the variables are set: ", category, latitude, longitude, full_address_string, image_url, business_name)

    db_resp = db_create_new_business_from_image(category, latitude, longitude, full_address_string, image_url, business_name)
    print(db_resp)

#     content = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/wifi-password/acopiocoffee_wifi.jpg#img"
#     content_type = "WiFi Image"
#     content_group = "Cellphone Image"
#     content_category = "Image"
#     content_url = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/wifi-password/acopiocoffee_wifi.jpg"
#     is_active = True
#     is_validated = True
#     last_scraped_datetime = datetime.now()
#     create_date = datetime.now()
#     created_by = "MCP Server"
#     modified_datetime = None
#     modified_by = None
#     venue_to_content = "acopiocoffee.com#https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/wifi-password/acopiocoffee_wifi.jpg#WEB"
#     venue_to_content_type = "WiFi"
#     venue = "VNU#acopiocoffee.com#WEB"
#     # establishment_to_content = "acopiocoffee.com#https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/wifi-password/acopiocoffee_wifi.jpg#WEB"
#     # establishment = "acopiocoffee.com#WEB"
#     # resp = db_image_to_establishment_content_upsert(content, content_type, content_group, content_category, content_url, is_active, last_scraped_datetime, create_date, created_by, modified_datetime, modified_by, establishment_to_content, establishment)
#     resp = db_image_to_venue_content_upsert(content, content_type, content_group, content_category, content_url, is_active, is_validated, last_scraped_datetime, create_date, created_by, modified_datetime, modified_by, venue_to_content, venue, venue_to_content_type)
# #     content_url = "https://campio2025flmobile.blob.core.windows.net/mobile-uploaded-images/wifi-password/acopiocoffee_wifi.jpg"
# #     venue = "VNU#acopiocoffee.com#WEB"
# #     WiFi_Network = 'Acopio Coffee'
# #     WiFi_Password = 'acopio2023'
# #     resp = db_upsert_wifi_network_password(venue, WiFi_Network, WiFi_Password, True, True, "MCP Server")
# #     # resp = db_create_new_business_from_image(category='Brewery', latitude=47.6062, longitude=-122.3321, image_url='testimageurl.com/image1.jpg')
# #     # resp = db_get_business_list_by_lat_lon(category='Brewery', latitude=47.6062, longitude=-122.3321, radius_miles=5.0, return_count=10)
# #     # resp = db_facebook_business_page_upsert(fburl, hpurl)
#     print(resp)
# #     # db_establish_upsert(Establishment, Establishment_Name, is_active, Process_user)


