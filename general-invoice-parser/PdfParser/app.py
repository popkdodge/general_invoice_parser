
import json
import  boto3
from gen_pdf_parser import text_tract_parser, pandas_to_dynamodb
from datetime import datetime
import os
import sys

PROCESSED_CSV_FILE_HANDLE_BUCKET = os.environ.get('PROCESSED_CSV_FILE_HANDLE_BUCKET')
PDF_FILE_HANDLE_BUCKET = os.environ.get('PDF_FILE_HANDLE_BUCKET')

sns = boto3.client('sns')
def lambda_handler(event, context):
    print(event)
    OBJECT_KEY = event['Records'][0]['s3']['object']['key']
    BUCKET_NAME = event['Records'][0]['s3']['bucket']['name']

    df = text_tract_parser({'BUCKET':BUCKET_NAME,'KEY':OBJECT_KEY})
    file_handle_name_s3 = df['file_handle_pdf_filename'].iloc[0]
    file_handle_name_files = file_handle_name_s3.replace('/','_')
    #DOES: save s3 link with new file handle
    s3_client = boto3.resource('s3')
    copy_source = {'Bucket': BUCKET_NAME, 'Key': OBJECT_KEY}
    # Incase we want to save the file handle in a different bucket
    # s3_file_handle_bucket = 'file-handle-bucket'
    s3_client.meta.client.copy(CopySource = copy_source, Bucket = PDF_FILE_HANDLE_BUCKET, Key = file_handle_name_files + '.pdf')

    #DOES: save s3 location with new file handle
    df['file_handel_s3_location'] = json.dumps({
        "KEY":file_handle_name_files + '.pdf' ,
        "BUCKET":PDF_FILE_HANDLE_BUCKET   
        })
    #DOES: SAVE TO S3 (renamed)=
    df.to_csv('/tmp/' + file_handle_name_files + '.csv', index=False)
    s3_client.meta.client.upload_file('/tmp/' + file_handle_name_files + '.csv', PROCESSED_CSV_FILE_HANDLE_BUCKET, file_handle_name_files + '.csv')

    #DOES: SAVE TO DYNAMODB
    pandas_to_dynamodb(df, 'general-invoice-data')
    return event
    