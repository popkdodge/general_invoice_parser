
import json
import  boto3
from winward_pdf_parser import winward_csv_parser
from gen_pdf_parser import text_tract_parser, pandas_to_dynamodb
from dotenv import load_dotenv
from datetime import datetime
from PyPDF2 import PdfFileReader, PdfFileWriter
import os
from io import BytesIO
import requests
import shutil
from dotenv import load_dotenv
from dateutil import parser

#GLOBAL_VAR
MODE =  os.environ.get("NSSandboxProductionMode")

UPLOAD_PDF_FOLDER_INTERNAL_ID =  os.environ.get("UPLOAD_PDF_FOLDER_INTERNAL_ID")
UPLOAD_CSV_FOLDER_INTERNAL_ID =  os.environ.get("UPLOAD_CSV_FOLDER_INTERNAL_ID")
ERROR_NOTIFICATION_ARN = os.environ.get("ERROR_NOTIFICATION_ARN")
CSV_SQS_QUEUE = os.environ.get('CSV_SQS_QUEUE')
BUCKET_NAME_ERRORS = os.environ.get('BUCKET_NAME_ERRORS')
NSSECRETS = '/winward-invoice-pdf-to-csv-upload-to-ns/key-secrets'
PROCESSED_CSV_FILE_HANDLE_BUCKET = os.environ.get('PROCESSED_CSV_FILE_HANDLE_BUCKET')
PDF_FILE_HANDLE_BUCKET = os.environ.get('PDF_FILE_HANDLE_BUCKET')

#Import Secrets
secrets_parameter = boto3.client('ssm')
parameter = secrets_parameter.get_parameter(Name=NSSECRETS, WithDecryption=True)
get_secrets = parameter['Parameter']['Value']
secrets = json.loads(get_secrets)

def send_message(message):
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.send_message(
        QueueUrl=CSV_SQS_QUEUE,
        MessageBody=json.dumps(message)
    )
    return response

def upload(conn, file_name, folder_internal_id:str, name=None):
    # Require Folder Name.
    # Require path the desired files.
    # Check Media types
    # Upload files with proper Media types require by NS.
    # Return Passing Response.

    # Check if folder Exist.
    print('Proper Folder Check!')
    internal_id = folder_internal_id
    search_file = None
    folders = conn.folders.get_all()
    print('Searching...')
    searched_folder = None
    for index, folder in enumerate(folders):
        if folder['internalId'] == internal_id:
            print('Folder located!')
            searched_folder = folder
    if searched_folder == None:
        return 'Folder Missing Error: Incorrect Folder InternalId.'
        
    folder_ref = {'internalId': searched_folder['internalId'], 'type': 'folder'}

    file_type = file_name.split('.')[-1]

    # Dictionary of Avaliable Mediatypes
    media_type_dict = {
        'xlsx':'_EXCEL',
        'csv':'_CSV',
        'pdf':'_PDF',
        'png':'_PNGIMAGE',
        'jpg':'_JPGIMAGE',
        'jpeg':'_JPGIMAGE'
    }
    if name == None:
        name = file_name
    # Checking for supported media types.
    print('Checking for supported media types.')
    if file_type in media_type_dict:

        media_type = media_type_dict[file_type]

        with open(file_name, "rb") as file:
            buf = BytesIO(file.read())

        file_to_be_upload_json = {
            'folder':folder_ref,
            'name':name,
            'externalId':name,
            'textFileEncoding':'_utf8',
            'FileAttachFrom':'_computer',
            'content': buf.getvalue() ,
            'mediaType': media_type,
            'isOnline': 'true'
        }
        print('Attempting to upload.')
        return conn.files.post(file_to_be_upload_json)
        
    elif file_type not in media_type_dict:
        return 'Invalid: Unsupported File types.'

    return "Try Checking Connection. or Reconnect."

sns = boto3.client('sns')
def lambda_handler(event, context):
    print(event)
    payload = event['Records'][0]
    print(payload)
    OBJECT_KEY = payload['s3']['object']['key']
    BUCKET_NAME = payload['s3']['bucket']['name']
    client = boto3.client('s3')
    temp_pdf_filename = '/tmp/to_be_parse.pdf'

    with open(temp_pdf_filename, 'wb') as f:
        client.download_fileobj(BUCKET_NAME, OBJECT_KEY,f)
    
    df = text_tract_parser({'BUCKET':BUCKET_NAME,'KEY':OBJECT_KEY})
    
    #DOES: save s3 link with new file handle
    s3_client = boto3.resource('s3')
    copy_source = {'Bucket': s3BucketName, 'Key': payload['KEY']}
    # Incase we want to save the file handle in a different bucket
    # s3_file_handle_bucket = 'file-handle-bucket'
    s3_client.meta.client.copy(CopySource = copy_source, Bucket = PDF_FILE_HANDLE_BUCKET, Key = file_handle_name)

    #DOES: save s3 location with new file handle
    df['file_handel_s3_location'] = json.dumps({
        "KEY":file_handle_name ,
        "BUCKET":s3BucketName   
        })
    #DOES: SAVE TO S3 (renamed)
    df.to_csv('/tmp/' + file_handle_name_files + '.csv', index=False)
    s3_client.meta.client.upload_file('/tmp/' + file_handle_name_files + '.csv', PROCESSED_CSV_FILE_HANDLE_BUCKET, file_handle_name_files + '.csv')

    #DOES: SAVE TO DYNAMODB
    pandas_to_dynamodb(df, 'general-invoice-data')
    return event
    
