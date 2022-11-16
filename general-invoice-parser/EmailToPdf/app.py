import boto3
import base64
import json
import email 
import os
from PyPDF2 import PdfFileReader, PdfFileWriter


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
    

    # Get the service client
    s3 = boto3.client('s3')
    sns = boto3.client('sns')
    # Download object at bucket-name with key-name to tmp.txt
    
    s3.download_file(BUCKET_NAME, EMAIL_OBJ_KEY , "/tmp/temp.txt")
    
    with open('/tmp/temp.txt') as f:
        data = f.read()
    
    message = email.message_from_string(data)
    attachment = message.get_payload()[1]
    # None Check For attachments.
    # assert
    try:
        assert attachment.get_content_type() is not None
        assert attachment.get_content_type() == 'application/pdf'
    except AssertionError:
        print(AssertionError)
        if message.is_multipart():
            for part in message.walk():
                ctype = part.get_content_type()
                cdispo = str(part.get('Content-Disposition'))

                # skip any text/plain (txt) attachments
                if ctype == 'text/plain' and 'attachment' not in cdispo:
                    body = part.get_payload(decode=True)  # decode
                    break
        # not multipart - i.e. plain text, no attachments, keeping fingers crossed
        else:
            body = message.get_payload(decode=True)
        response = sns.publish(
            TargetArn = ERROR_NOTIFICATION_ARN,
            Subject='Missing Attachments',
            Message = f"""
        The following email was received by gumps999@robo-gumps.com , but we were expecting an attachment, please review:

            Email Reference:

                Timestamp: {message['Date']}

                From: {message['From']}

                To: {message['To']}

                Subject: {message['Subject']}

                Body: {body}


            """)
        return "Wrong or None Attachments Error"

    file_name = attachment.get_filename()
    file_bytes = attachment.get_payload()
    content_type = attachment.get_content_type()
    
    with open("/tmp/"+file_name, "wb") as f:
        f.write(base64.b64decode(file_bytes))
        
    temp_pdf_filename = "/tmp/"+file_name
    OBJECT_KEY = file_name
    s3.upload_file(temp_pdf_filename, BUCKET_NAME, 
        OBJECT_KEY,
        ExtraArgs={'ContentType': content_type}
        )
    payload = {
        "BUCKET": BUCKET_NAME,
        "KEY": OBJECT_KEY
            
    }
    send_message(payload)
    print("appending to queue")
    response = {
        "STATUS_CODE":200,
        "COMMENTS": "SUCCESS",
    }
    return response
    

