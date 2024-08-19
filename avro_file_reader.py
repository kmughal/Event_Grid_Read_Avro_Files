import fastavro
import json

def read_avro_file(file_path):
    """
    Reads an Avro file and returns its contents as a list of JSON objects.
    
    Args:
        file_path (str): The path to the Avro file to be read.
    
    Returns:
        list: A list of dictionaries representing the records in the Avro file.
    
    This function reads an Avro file, converts each record to a pretty-printed JSON string,
    prints it, and appends the original record (as a dictionary) to a list, which is then returned.
    """
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        records = []
        for record in reader:
            pretty_json = json.dumps(record, indent=4)
            print(pretty_json)
            records.append(record)
    return records