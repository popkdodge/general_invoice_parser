import json
import csv
import os
import boto3
from netsuitesdk import NetSuiteConnection
import requests
import shutil
from io import BytesIO
import datetime
from dateutil import parser


MODE =  os.environ.get("NSSandboxProductionMode")

UPLOAD_PDF_FOLDER_INTERNAL_ID =  os.environ.get("UPLOAD_PDF_FOLDER_INTERNAL_ID")
UPLOAD_CSV_FOLDER_INTERNAL_ID =  os.environ.get("UPLOAD_CSV_FOLDER_INTERNAL_ID")
ERROR_NOTIFICATION_ARN = os.environ.get("ERROR_NOTIFICATION_ARN")
CSV_SQS_QUEUE = os.environ.get('CSV_SQS_QUEUE')
BUCKET_NAME_ERRORS = os.environ.get('BUCKET_NAME_ERRORS')
SUCCESS_SNS_TOPIC = os.environ.get('SUCCESS_SNS_TOPIC')
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
            return "Connection Failed"

    else:
        return "Invalid modes selection! Choose either 'production' or 'test'."

    return "Something went wrong. Try again."

def send_message(message):
    client = boto3.client('sns')

    response = client.publish(
        TopicArn=SUCCESS_SNS_TOPIC,    
        Message= message
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
client = boto3.client('s3')
def lambda_handler(event, context):
    """
    """
    print(event)
    list_of_csv = []
    list_of_pdf = []
    records = event['Records']
    for record in records:
        dict_record = json.loads(record['body'])
        pdf = dict_record['PDF']

        csv_list = dict_record['csv_line']
        for csv_line in csv_list:
            pdf = dict_record['PDF']
            internal_id = dict_record['InternalId']
            date_time = parser.parse(csv_line['INVOICE Date'])
            time = date_time.strftime('%m/%d/%Y')
            csv_line['INVOICE Date'] = time

            date_time = parser.parse(csv_line['PO Date'])
            time = date_time.strftime('%m/%d/%Y')
            csv_line['PO Date'] = time

            date_time = parser.parse(csv_line['Date'])
            time = date_time.strftime('%m/%d/%Y')
            csv_line['Date'] = time

            csv_line['file_name'] = pdf
            csv_line['file_internal_id'] = internal_id
            try:
                csv_line['SKU'] = csv_line['SKU'].strip()
            except:
                csv_line['SKU'] = csv_line['SKU']
            list_of_pdf.append(pdf)
            list_of_csv.append(csv_line)

    keys = list_of_csv[0].keys()
    temp_csv_filename = '/tmp/temp.csv'
    netsuites_filename = f'PO_{datetime.datetime.now().strftime("%d_%m_%Y-%H%M%S")}_PROCESSED.csv'
    with open(temp_csv_filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(list_of_csv)

    connection = connect(MODE)
    try:
        upload_response = upload(connection, temp_csv_filename, UPLOAD_CSV_FOLDER_INTERNAL_ID, netsuites_filename)
        print(upload_response)
    except:
        client.upload_file(temp_csv_filename, BUCKET_NAME_ERRORS, "fail_csv/" + netsuites_filename,
                            ExtraArgs={'ContentType': "application/pdf"}
                            )
        sns.publish(
            TargetArn = ERROR_NOTIFICATION_ARN,
            Subject = 'Fail To Upload To NS',
            Message = f"""
            The following files failed to upload to Netsuite
            and need to be inspected before resending:

            {"https://invoicetocsvparseruploadns-errorstoragebucket-1l7aasat0lfc5.s3.amazonaws.com/"
                                +"fail_csv/"+netsuites_filename.lstrip("/tmp/")}

            If the document above appears correct, please download the file, attach, and resend to : gumps999@robo-gumps.com
                            """
                    )

    # Success message
    sns.publish(
        TargetArn = SUCCESS_SNS_TOPIC,
        Subject = 'Successful PDF Processed',
        Message = f"""
        List of pdf upload to vendor invoice: {list_of_pdf}
        File uploaded to RIEM Imports: {netsuites_filename}
        """
        )
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
            "message": f""" 
            List of pdf upload to vendor invoice: {list_of_pdf}

            File uploaded to RIEM Imports: {netsuites_filename}
            """
            }
        ),
    }
