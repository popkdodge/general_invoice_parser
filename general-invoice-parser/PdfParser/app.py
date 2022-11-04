
import json
import  boto3
from winward_pdf_parser import winward_csv_parser
import json
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
from PyPDF2 import PdfFileReader, PdfFileWriter
import os
from io import BytesIO
from netsuitesdk import NetSuiteConnection
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

#Import Secrets
secrets_parameter = boto3.client('ssm')
parameter = secrets_parameter.get_parameter(Name=NSSECRETS, WithDecryption=True)
get_secrets = parameter['Parameter']['Value']
secrets = json.loads(get_secrets)

# Production
NS_CONSUMER_KEY = secrets['NS_CONSUMER_KEY']
NS_CONSUMER_SECRET = secrets['NS_CONSUMER_SECRET']
NS_TOKEN_ID = secrets['NS_TOKEN_ID']
NS_TOKEN_SECRET = secrets['NS_TOKEN_SECRET']
NETSUITE_ACCOUNT_ID = secrets['NETSUITE_ACCOUNT_ID']

# Sandbox
TEST_CONSUMER_KEY = secrets['TEST_CONSUMER_KEY']
TEST_CONSUMER_SECRET = secrets['TEST_CONSUMER_SECRET']
TEST_TOKEN_ID = secrets['TEST_TOKEN_ID']
TEST_TOKEN_SECRET = secrets['TEST_TOKEN_SECRET']
TEST_ACCOUNT_ID = secrets['TEST_ACCOUNT_ID']


def connect(modes: str):
    if modes.lower() == 'test':

        nc = NetSuiteConnection(
        account=TEST_ACCOUNT_ID,
        consumer_key=TEST_CONSUMER_KEY,
        consumer_secret=TEST_CONSUMER_SECRET,
        token_key=TEST_TOKEN_ID,
        token_secret=TEST_TOKEN_SECRET,
        caching=False
        )
        try:
            return nc
        except:
            return "Connection Failed :("

    elif modes.lower() == 'production':

        nc = NetSuiteConnection(
            account=NETSUITE_ACCOUNT_ID,
            consumer_key=NS_CONSUMER_KEY,
            consumer_secret=NS_CONSUMER_SECRET,
            token_key=NS_TOKEN_ID,
            token_secret=NS_TOKEN_SECRET,
            caching=False
        )
        
        try:
            return nc

        except:
            return "Connection Failed :("

    else:
        return "Invalid modes selection! Choose either 'production' or 'test'."

    return "Something went wrong. Try again."

def send_message(message):
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.send_message(
        QueueUrl=CSV_SQS_QUEUE,
        MessageBody=json.dumps(message)
    )
    return response

def download(conn , file_name):
    # Input desired file name.
    # Find Files
    search_file = None
    files = conn.files.get_all()
    print('Searching...')
    for index, file in enumerate(files):
        if file['name'] == file_name:
            print('Files located!')
            search_file = file
            
    if search_file == None:
        return 'File Searching Failed: Check for Case-sensitivity or proper filenames.'
            
    url = search_file.url
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    r = requests.get(url, headers=headers, stream = True)

    if r.status_code == 200:
        # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
        r.raw.decode_content = True
        print('Downloading..')
        # Open a local file with wb ( write binary ) permission.
        with open(file_name,'wb') as f:
            shutil.copyfileobj(r.raw, f)
        
        return f"Success: Check {file_name} current directory."
    return "Try Checking Connection. or Reconnect."
    
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
    payload = json.loads(event['Records'][0]['body'])
    print(payload)
    client = boto3.client('s3')
    temp_pdf_filename = '/tmp/to_be_parse.pdf'

    with open(temp_pdf_filename, 'wb') as f:
        client.download_fileobj(payload["BUCKET"], payload["KEY"],f)
    csv = winward_csv_parser(temp_pdf_filename)
    csv_json  = csv.loc[0].to_dict()
    date_time = parser.parse(csv_json['INVOICE Date']).strftime("%m-%d-%Y")
    invoice_name = f"{date_time}_INV_{csv_json['Invoice #']}_{csv_json['PO #']}.pdf"
    # Check for missing item.

    list_of_required_columns = ['PO Date', 
        'INVOICE Date', 
        'Expected Ship Date',
        'PO #',
        'Rate',
        'Qty', 'Vendor SKU #', 'TRACKING', 'Invoice #']

    dict_of_errors = {}

    for col in list_of_required_columns:
        try:
            assert csv[csv[col].isnull()].shape[0] == 0
        except AssertionError:
            dict_of_errors[col] = csv[csv[col].isnull()].index.to_list()

    list_of_missing_fields_idx =[]
    list_of_missing_fields_reason = []
    message_for = "Missing Fields: Page Number\n"
    for key in dict_of_errors.keys():
        # Replacing Empty Expected Ship Date with Invoice Date
        if key == 'Expected Ship Date':
            for idx in dict_of_errors[key]:
                csv.at[idx, key] = csv.loc[idx, 'INVOICE Date']
            # Message that Expected Date is Replace by Invoice Date but still process as normal.
        # Removing missing columns and notifiy.
        else:
            for idx in dict_of_errors[key]:
                list_of_missing_fields_idx.append(idx)
                list_of_missing_fields_reason.append(key)
                # Add to message notifiying reason to remove from csv.
                message_for = message_for + f"{key}: {idx+1}\n"
     
    file = f'Failed_PDF_{datetime.now().strftime("%d_%m_%Y-%H%M%S")}.pdf'   

    if len(list_of_missing_fields_idx) > 0:
        s3 = boto3.client('s3')
        s3.upload_file(temp_pdf_filename, BUCKET_NAME_ERRORS, "fail_pdf/"+file,  ExtraArgs={'ContentType': "application/pdf",'ACL': 'public-read'})
        # - notify failure SNS Topic.
        sns.publish(
            TargetArn = ERROR_NOTIFICATION_ARN,
            Subject = 'Invoice Contain an Error',
            Message = f"""
        The following mandatory field value was not found: {list_of_missing_fields_reason[0]}
        and need to be inspected before resending:


        Please use the link below to inspect the document before resubmitting:

        https://{BUCKET_NAME_ERRORS}.s3.amazonaws.com/fail_pdf/{file}

        If you feel that this notification was sent in error, please forward this message to: it@gumps.com
        Otherwise resubmit the corrected PDF via email to : gumps999@robo-gumps.com
        """
                                )
        return "ERRORFOUND"

    csv.columns = ['PO Date', 
                    'INVOICE Date',
                    'Date', 
                    'PO', 
                    'Rate', 
                    'Qty',
                    'Vendor Style #', 
                    'TRACKING', 
                    'Document Number',
                    'SKU']
    csv = csv[['PO Date', 
                    'INVOICE Date','Date', 
                    'PO', 
                    'Rate', 
                    'Qty',
                    'TRACKING', 
                    'Document Number',
                    'Vendor Style #', 
                    'SKU']].copy()

    row_count = csv.shape[0]
    list_of_rows = []
    for count in range(row_count):
        list_of_rows.append(csv.loc[count].to_dict())
    connection = connect(MODE)
    try:
        upload_response = upload(connection, temp_pdf_filename, UPLOAD_PDF_FOLDER_INTERNAL_ID, invoice_name)
        print(upload_response)
    except:
        client.upload_file(temp_pdf_filename, BUCKET_NAME_ERRORS, 
            payload["KEY"],
            ExtraArgs={'ContentType': "application/pdf"}
            )
        sns.publish(
            TargetArn = ERROR_NOTIFICATION_ARN,
            Subject = 'Fail To Upload To NS',
            Message = f"""
            The following files failed to upload to Netsuite
            and need to be inspected before resending.:

            https://{BUCKET_NAME_ERRORS}.s3.amazonaws.com/fail_pdf/{file}

            If the document above appears correct, please download the file, attach, and resend to : gumps999@robo-gumps.com
                            """
                    )

    payload = {
        "csv_line": list_of_rows,
        "PDF": invoice_name,
        "InternalId": upload_response['internalId']
                
        }
    response = send_message(payload)
    print(f'Success: {response}')
    return response
    