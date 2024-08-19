import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import fastavro
import json

app = func.FunctionApp()
blob_service_client = BlobServiceClient.from_connection_string(app.get_setting('AzureWebJobsStorage'))

@app.event_grid_trigger(arg_name="azeventgrid")
def AvroFilesRead_EventGrid(azeventgrid: func.EventGridEvent):
    """
    Event Grid trigger function that processes an event when a new Avro file is added to the storage account.
    
    Args:
        azeventgrid (func.EventGridEvent): The Event Grid event that triggered the function.
    
    Raises:
        ValueError: If the event does not contain a URL.
    
    Returns:
        list: A list of dictionaries representing the records in the Avro file.
    
    This function:
    1. Extracts the URL of the newly added Avro file from the Event Grid event.
    2. Downloads the Avro file from the storage account.
    3. Reads the Avro file and converts its contents to JSON.
    4. Logs the pretty-printed JSON data.
    """
    logging.info('Python EventGrid trigger processed an event')
    
    # Extract the URL of the newly added Avro file from the event
    avro_file_url = azeventgrid.get_json()['data']['url']
    if avro_file_url is None:
        raise ValueError('The event does not contain a URL.')
    
    # Extract the container name and blob name from the URL
    container_name = avro_file_url.split('/')[3]
    blob_name = avro_file_url.split('/')[4]
    
    # Get the container and blob clients
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    
    # Download the Avro file and read its contents
    with blob_client.download_blob() as blob:
        reader = fastavro.reader(blob)
        records = []
        for record in reader:
            pretty_json = json.dumps(record, indent=4)
            logging.info(pretty_json)
            records.append(record)
    
    return records