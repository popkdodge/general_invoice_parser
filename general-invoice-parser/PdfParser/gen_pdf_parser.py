import pandas as pd
import boto3
import json
import awswrangler as wr
from decimal import Decimal


list_of_fields = [
    'INVOICE_RECEIPT_ID',
    'INVOICE_RECEIPT_DATE',
    'PO_NUMBER',
    'VENDOR_NAME',
    'CUSTOMER_NUMBER',
    'ORDER_DATE',
    'VENDOR_URL'
]

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
def float_to_decimal(num):
    return Decimal(str(num))

def pandas_to_dynamodb(df, table_name):
    df = df.fillna('N/A')
    # convert any floats to decimals
    for i in df.columns:
        datatype = df[i].dtype
        if datatype == 'float64':
            df[i] = df[i].apply(float_to_decimal)
    # write to dynamodb
    wr.dynamodb.put_df(df=df, table_name=table_name)
    print('Data written to dynamodb')


def text_tract_parser(payload):
    documents = [
        payload['KEY']
        ]
    s3BucketName = payload['BUCKET']


    extracted_fields = {}
    for document in documents:
        print(document)
        try:
            response = textractmodule.analyze_expense(
                    Document={
                        'S3Object': {
                            'Bucket': s3BucketName,
                            'Name': document, 
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
                            'Confidence': Decimal(fields['Type']['Confidence'])

                    }
                if fields['Type']['Text'] not in list_of_fields:
                    if fields['Type']['Text'] not in key_fields_value:
                        key_fields_value[fields['Type']['Text']] = [{
                            'Text': fields['ValueDetection']['Text'],
                            'Confidence': Decimal(fields['Type']['Confidence'])}]
                    
                    if fields['Type']['Text'] in key_fields_value:
                        key_fields_value[fields['Type']['Text']].append({
                            'Text': fields['ValueDetection']['Text'],
                            'Confidence': Decimal(fields['Type']['Confidence'])

                        })
                    deduplicate = key_fields_value[fields['Type']['Text']]
                    key_fields_value[fields['Type']['Text']] = [dict(t) for t in {tuple(d.items()) for d in deduplicate}]

            extracted_fields[document] = key_fields_value
        except:
            print('Error in extracting fields from document: ', document)


    df = pd.DataFrame(extracted_fields).T

    df = df.reset_index()

    #df.columns = ['file_name', 'INVOICE_RECEIPT_ID', 'INVOICE_RECEIPT_DATE', 'PO_NUMBER',
    #      'VENDOR_NAME', 'CUSTOMER_NUMBER', 'ORDER_DATE', 'VENDOR_URL']
    df = df[['index']+list_of_fields+[x for x in list(df.columns) if x not in list_of_fields + ['index']]]


    for column in df.columns:
        if column in list_of_fields:
            df[column] = df[column].apply(extract_text_from_dictionary)
            
    df = df.rename(columns={'index':"DocumentName","VENDOR_NAME": "vendor", 
        "INVOICE_RECEIPT_ID": "invoice_num", 
        'PO_NUMBER': "po_number", "INVOICE_RECEIPT_DATE":"invoice_date"})

    #df = df[['vendors','invoice_number','invoice_date', 'po_number','DocumentName',
    #    'CUSTOMER_NUMBER', 'ORDER_DATE', 'VENDOR_URL', 'ADDRESS',
    #    'ADDRESS_BLOCK', 'AMOUNT_DUE', 'AMOUNT_PAID', 'CITY', 'NAME', 'OTHER',
    #    'PAYMENT_TERMS', 'RECEIVER_ADDRESS', 'RECEIVER_NAME', 'STATE', 'STREET',
    #    'SUBTOTAL', 'TOTAL', 'VENDOR_ADDRESS', 'VENDOR_PHONE', 'ZIP_CODE']]
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])
    df['invoice_date'] = df.invoice_date.astype('int64')
    pandas_to_dynamodb(df, 'general-invoice-data')

    return df
