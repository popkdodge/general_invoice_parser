import boto3
import base64
import json
import email 
import os
from PyPDF2 import PdfFileReader, PdfFileWriter
from datetime import datetime

def loggings(Name, Event, Info='Info:'):
    """_summary_: This function is used to log the events in the cloudwatch logs

    Args:
        Name (_type_): _description_
        Event (_type_): _description_
        Info (str, optional): _description_. Defaults to 'Info:'.
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(f'GumpsStacksLog') 
    dt = datetime.now()
    table.put_item(Item={
        'Name': Name,
        'Timestamp': int(datetime.timestamp(dt)),
        'Event': Event,
        'Info': 'Info:'

    })


def send_message(message):
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.send_message(
        QueueUrl=CSV_SQS_QUEUE,
        MessageBody=json.dumps(message),
        MessageGroupId="messageGroup1"
    )
    print(response)

def separate_pdf(pdf):
    """Helper functions: Split pdf by Page per pages.

    Args:
        pdf (path): _path to pdf_
        list_po (list): _list of PO #_
        list_inv_date (list): _list of inventory date_
        list_inv_name (list): _list of inventory name_

    Returns:
        _list of file name_: _list type list: of splitted filename_
    """
    fname = pdf 
    path = pdf
    pdf = PdfFileReader(path)
    list_of_files_name = []
    for page in range(pdf.getNumPages()):
        pdf_writer = PdfFileWriter()
        pdf_writer.addPage(pdf.getPage(page))

        output_filename = f'/tmp/{fname.strip("/tmp/").strip(".pdf")}_INV_{str(page)}.pdf'

        with open(output_filename, 'wb') as out:
            pdf_writer.write(out)
        list_of_files_name.append(output_filename)
        
    return list_of_files_name

SAVE_PDF_PREFIX = os.environ['SAVE_PDF_PREFIX'] 
ERROR_NOTIFICATION_ARN = os.environ['ERROR_NOTIFICATION_ARN']
CSV_SQS_QUEUE = os.environ['CSV_SQS_QUEUE']

def lambda_handler(event, context):
    print(event)
    records = event['Records'][0] 
    res = json.loads(records['Sns']['Message'])
    EMAIL_OBJ_KEY = res['receipt']['action']['objectKey']
    BUCKET_NAME = res['receipt']['action']['bucketName']
    PREPROCESSING_BUCKET = os.environ['PREPROCESSING_BUCKET']
    

    # Get the service client
    s3 = boto3.client('s3')

    # Download object at bucket-name with key-name to tmp.txt
    s3.download_file(BUCKET_NAME, EMAIL_OBJ_KEY , "/tmp/temp.txt")
    with open('/tmp/temp.txt') as f:
        data = f.read()
    
    # Extract the email contents
    payloads = email.message_from_string(data)
    # Isolate Content-Type
    list_of_allowed_content_types = ['application/pdf']

    # Iterate through the payloads to find content matching the allowed content types
    for payload in payloads.get_payload():
        if payload.get_content_type() in list_of_allowed_content_types:
            print("Following Content-type found:",payload.get_content_type())
            print('Attachments with the filename:',payload.get_filename(), 'detected!')
            file_name = payload.get_filename()
            file_bytes = payload.get_payload()
            content_type = payload.get_content_type()

            # Save in temp file to be uploaded to S3
            with open("/tmp/"+file_name, "wb") as f:
                f.write(base64.b64decode(file_bytes))
            temp_pdf_filename = "/tmp/"+file_name
            list_of_file_name = separate_pdf(temp_pdf_filename)
            for file in list_of_file_name:
            # Naming convention for the file to be uploaded to S3
                OBJECT_KEY = "pre-processing-bucket/" + file.lstrip('/tmp/')
                s3.upload_file(file, PREPROCESSING_BUCKET, 
                    OBJECT_KEY,
                    ExtraArgs={'ContentType': content_type}
                    )
                #Log successful s3 upload
                #TODO:
                    # loggings('EmailToPdf', 'File Uploaded to S3', OBJECT_KEY)
                loggings('GENERALINVOICEPARSER_EMAILTOPDF', 'SUCCESS_FILE_UPLOADED', "File Uploaded to S3: " + OBJECT_KEY)
                print(f"{file} is uploaded to S3 bucket {PREPROCESSING_BUCKET} with key {OBJECT_KEY}")

    return None
    

