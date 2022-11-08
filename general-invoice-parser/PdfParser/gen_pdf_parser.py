import pandas as pd
import boto3
import json

textractmodule = boto3.client('textract')

def extract_fields_from_expense_document(documents_name, list_of_fields):
    response = textractmodule.analyze_expense(
            Document={
                'S3Object': {
                    'Bucket': s3BucketName,
                    'Name': documents_name
                    }})
    key_fields_value = {}
    for field in list_of_fields:
        key_fields_value[field] = {
            'Text': None,
            'Confidence': 0
        }
    for fields in response['ExpenseDocuments'][0]['SummaryFields']:
        if fields['Type']['Text'] in list_of_fields:
            if fields['Type']['Confidence'] > key_fields_value[fields['Type']['Text']]['Confidence']:
                key_fields_value[fields['Type']['Text']] = {
                    'Text': fields['ValueDetection']['Text'],
                    'Confidence': fields['Type']['Confidence']

            }
    return key_fields_value

def extract_text_from_dictionary(dictionary):
    return dictionary['Text']


#TODO: get documents from events.

#TODO: Add a function to save blobs to s3 and get the s3 path.

#TODO: Add a function to save s3 links as part of the data.



extracted_fields = {}
for document in documents:
    print(document)
    try:
        extracted_fields[document] = extract_fields_from_expense_document(document, list_of_fields)
    except:
        print('Error in extracting fields from document: ', document)


df = pd.DataFrame(extracted_fields).T


df['INVOICE_RECEIPT_ID'] = df['INVOICE_RECEIPT_ID'].apply(extract_text_from_dictionary)

for column in df.columns:
    df[column] = df[column].apply(extract_text_from_dictionary)
df = df.reset_index()

df.columns = ['file_name', 'INVOICE_RECEIPT_ID', 'INVOICE_RECEIPT_DATE', 'PO_NUMBER',
       'VENDOR_NAME', 'CUSTOMER_NUMBER', 'ORDER_DATE', 'VENDOR_URL']

df