import json
import csv
from datetime import datetime
from dateutil import tz

class json_flatner:

    # Static method to read JSON file
    @staticmethod
    def read_json(jspath):
        with open(jspath, 'r') as dictFile:
            myDict = json.load(dictFile)
        return myDict

    # Initialize an empty list to store table rows
    table_data = []

    

    # Helper function to convert the given datetime string to UTC
    @staticmethod
    def convert_to_utc(datetime_str):
        # Parse the datetime string with the timezone info and convert to UTC
        dt_with_tz = datetime.fromisoformat(datetime_str)
        dt_utc = dt_with_tz.astimezone(tz.UTC)
        return dt_utc.strftime('%Y-%m-%d %H:%M:%S')

    # Function to extract the collection date
    @staticmethod
    def extract_collection_date(data, filename):
        data_date = None
        file_creation_datetime = None

        # Traverse the data to find the Data_Date and File_Creation_Datetime
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "Data_Date":
                    data_date = value
                elif key == "File_Creation_Datetime":
                    file_creation_datetime = value
                else:
                    # Recursively check nested dictionaries
                    if isinstance(value, dict) or isinstance(value, list):
                        found_date = json_flatner.extract_collection_date(value, filename)
                        if found_date:
                            return found_date

        # Primary Source: Data_Date
        if data_date:
            return json_flatner.convert_to_utc(data_date)
        
        # Secondary Source: File_Creation_Datetime
        if file_creation_datetime:
            return json_flatner.convert_to_utc(file_creation_datetime)
        
        # Tertiary Source: Extract from filename if date is embedded
        try:
            date_from_filename = datetime.strptime(filename, '%Y-%m-%d').date()
            return date_from_filename.strftime('%Y-%m-%d 00:00:00')
        except ValueError:
            # Handle if the filename does not contain a valid date
            return None

    # Recursive function to traverse and collect terminal nodes with their path, attribute name, and value
    @staticmethod
    def get_terminal(data, parent_path="", filename=""):
        if isinstance(data, dict):
            for key, value in data.items():
                current_path = f"{parent_path}~>{key}"
                json_flatner.get_terminal(value, current_path, filename)
        elif isinstance(data, list):
            for i, item in enumerate(data):
                current_path = f"{parent_path}-{i+1}"
                json_flatner.get_terminal(item, current_path, filename)
        else:
            # Extract necessary data for terminal nodes
            attribute_name = parent_path.split("~>")[-1]  # Get the immediate parent attribute name
            cleaned_path = "~>".join(parent_path.split("~>")[:-1])  # Remove the immediate parent node from the path
            tgt_object_class_instance = parent_path.split("~>")[-2]  # Get the last root from the cleaned path

            # Extract target object class
            if '-' in tgt_object_class_instance:
                tgt_object_class = tgt_object_class_instance.split('-')[0]  # Remove suffix if present
            else:
                tgt_object_class = tgt_object_class_instance

            # Get collection_date_utc using the helper function
            collection_date_utc = json_flatner.extract_collection_date(data, filename)

            # Append the extracted data to the table
            json_flatner.table_data.append({
                "collection_date_utc":collection_date_utc,
                "tgt_object_class": tgt_object_class,
                "tgt_object_class_instance": tgt_object_class_instance,
                "tree_path": cleaned_path, 
                "attribute_name": attribute_name, 
                "attribute_value": data,
                
            })


    # Function to export collected terminal nodes to a CSV file
    @staticmethod
    def export_to_csv():
        with open("./data/processed_output.csv", mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=["collection_date_utc","tgt_object_class","tgt_object_class_instance","tree_path", "attribute_name", "attribute_value"])
            writer.writeheader()  # Write the header to the CSV file
            writer.writerows(json_flatner.table_data)  # Write the rows from table_data

    def get_file_name(filename):
        final_filename='JsonSplit_TEST'
        return final_filename
    
    def get_timestamp_from_file(filename):
        final_timestamp='20240811074124'
        return final_timestamp

    def convert_to_utc_timestamp(Data_Date, File_Creation_Datetime, File_Timestamp):
        #if datadate - get to utc
        #elif file-creation-datetime - get 
        #else both are missing - get from file name
        pass


# Read the JSON
data = json_flatner.read_json("./data/JsonSplit_TEST_20240811074124.json")

# Print the JSON tree
json_flatner.get_terminal(data)
json_flatner.export_to_csv()


# 2. collection_date_utc:
# Mapping Logic:
# Primary Source: Extract the Data_Date from TEST_Log -> HW_Info -> Data_Date.
# Secondary Source: If Data_Date is missing, use File_Creation_Datetime from TEST_Log -> HW_Info -> File_Creation_Datetime.
# Tertiary Source: If neither Data_Date nor File_Creation_Datetime is available, extract the date from the filename.