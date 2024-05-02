import azure.functions as func
import logging
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
from PyPDF2 import PdfReader,PdfWriter  # in order to read and write  pdf file 
import io # in order to download pdf to memory and write into memory without disk permission needed 
import json # in order to use json 
import pyodbc #for sql connections 
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 

# Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')

#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'


# Generic Function to update case  in the 'cases' table
def update_case_generic(caseid,field,value):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute(f"UPDATE cases SET {field} = ? WHERE id = ?", (value, caseid))
        conn.commit()

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info(f"case {caseid} updated field name: {field} , value: {value}")
        return True
    except Exception as e:
        logging.error(f"Error update case: {str(e)}")
        return False    

# Insert files into table "documents"
def insert_documents(caseid,filename,status,path,url):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new doc data into the 'documents' table
        cursor.execute(f"INSERT INTO documents (caseid, fileName, status, path, url) VALUES (?, ?, ?, ?, ?)", (caseid, filename, status, path,url))

        conn.commit()

        # Get the ID of the last inserted row
        cursor.execute("SELECT @@IDENTITY AS 'Identity';")
        doc_id = cursor.fetchone()[0]

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info(f"insert New Documnets successfully, documents id is:  {doc_id} , caseid is : {caseid}")
        return True
    except Exception as e:
        logging.error(f"Error update case: {str(e)}")
        return False      
    
#Create event on azure service bus 
def create_servicebus_event(queue_name, event_data):
    try:
        # Create a ServiceBusClient using the connection string
        servicebus_client = ServiceBusClient.from_connection_string(connection_string_servicebus)

        # Create a sender for the queue
        sender = servicebus_client.get_queue_sender(queue_name)

        with sender:
            # Create a ServiceBusMessage object with the event data
            message = ServiceBusMessage(event_data)

            # Send the message to the queue
            sender.send_messages(message)

        print("Event created successfully.")
    
    except Exception as e:
        print("An error occurred:", str(e))

#function split pdf into pages 
def split_pdf_pages(caseid,file_name):
    try:
        logging.info(f"split_pdf_pages caseid value is: {caseid}")
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        path = f"{basicPath}/source/{file_name}"
        logging.info(f"full path is : {path}")
        blob_client = container_client.get_blob_client(path)
        #check if file Exists
        fileExist = blob_client.exists()
        logging.info(f"fileExist value is: {fileExist}")
        if fileExist==False:
           return "not found file"
        # Download the blob into memory
        download_stream = blob_client.download_blob()
        pdf_bytes = download_stream.readall()
        # Open the PDF from memory
        pdf_file = io.BytesIO(pdf_bytes)  
        # Create PdfFileReader object
        pdf_reader = PdfReader(pdf_file)
        # Get number of pages
        num_pages = len(pdf_reader.pages)
        logging.info(f"num_pages value: {num_pages}")
        # Create directory if it doesn't exist
        baseDestination_path = f"{basicPath}/source/split"
        logging.info(f"destination_path value is: {baseDestination_path}")
        # Save each page as a separate file
        for i, page in enumerate(pdf_reader.pages):
            writer = PdfWriter()
            writer.add_page(page)
            # Get the bytes of the PDF page
            page_bytes = io.BytesIO()
            writer.write(page_bytes)
            page_bytes.seek(0)
            newFileName = f"page_{i+1}.pdf"
            Destination_path=f"{baseDestination_path}/{newFileName}"
            blob_client = container_client.upload_blob(name=Destination_path, data=page_bytes.read())
            insert_documents(caseid,newFileName,1,Destination_path,blob_client.url) #status = 1 split 
            #preparing data for service bus 
            data = { 
                "caseid" : caseid, 
                "filename" :newFileName,
                "path" :Destination_path,
                "url" :blob_client.url
            } 
            json_data = json.dumps(data)
            create_servicebus_event("ocr",json_data)
        logging.info(f"split_pdf_pages process: succeeded")
        data = { 
            "status" : "succeeded", 
            "pages_num" : num_pages,
            "Description" : f"split_pdf_pages process: succeeded ,Total Pages:{num_pages}" 
        } 
        json_data = json.dumps(data)
        return json_data
    except Exception as e:
        return str(e)
    
app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="split",
                               connection="medicalanalysis_SERVICEBUS") 
def sb_split_process(azservicebus: func.ServiceBusMessage):
    message_data = azservicebus.get_body().decode('utf-8')
    logging.info('Received messageesds: %s', message_data)
    message_data_dict = json.loads(message_data)
    caseid = message_data_dict['caseid']
    file_name = message_data_dict['filename']
    splitResult = split_pdf_pages(caseid,file_name)
    splitResult_dic = json.loads(splitResult)
    split_status = splitResult_dic['status']
    split_pages = splitResult_dic['pages_num']
    if split_status =="succeeded":
        #update case status to file split
        update_case_generic(caseid,"status",4) 
        
        logging.info(f"split status is: {split_status}, Total Pages is: {split_pages}")
    else: 
        logging.info(f"split status is: {split_status}")

