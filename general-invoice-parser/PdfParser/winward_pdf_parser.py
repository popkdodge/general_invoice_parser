import re
import pdfplumber
import pandas as pd

def useRegex(input):
    pattern = re.search('[0-9]{12}', input)
    return pattern
def useRegex2(input):
    pattern = re.search('U[0-9]{6}', input)
    return pattern
def useRegex3(input):
    pattern = re.compile(r"[a-zA-Z]+ [a-zA-Z]+: [0-9]+", re.IGNORECASE)
    return pattern.match(input)

def split_surcharge(df):
    shipped2 = [x for x in df.loc()[0].SHIPPED.split('       ')]
    items2 = [x for x in df.loc()[0].ITEM.strip().split(' ') if len(x)>1]
    prices2 = [x for x in df.loc()[0].PRICE.strip().split(' ') if len(x)>1]
    if df.loc()[0].SURCHARGE != 0.0:
        items2.append("DROPSHIP_SURCHARGE")
        prices2.append(df.loc()[0].SURCHARGE)
        shipped2.append('0')
    for i, item in enumerate(items2):
        if i == 0:
            df.loc()[0,['ITEM']] = items2[i]
            df.loc()[0,['PRICE']] = prices2[i]
            df.loc()[0,['SHIPPED']] = shipped2[i]
        else:
            main_copy = df.loc()[[0]].copy()
            main_copy.loc()[0,['ITEM']] = items2[i]
            main_copy.loc()[0,['PRICE']] = prices2[i]
            main_copy.loc()[0,['SHIPPED']] = shipped2[i]
            df = pd.concat([df, main_copy])

    return df.drop_duplicates()

def winward_csv_parser(pdf_filename):
    """_Parse PDF and return a extracted information in dataframes_

    Args:
        pdf_filename (path): _pad_

    Returns:
        _main_: dataframes containing extracted information from the pdf"""

    with pdfplumber.open(pdf_filename) as pdf:
        for page_index, pages in enumerate(pdf.pages):
            current_page = pdf.pages[page_index]

            table = current_page.extract_table()
            table[2][6] = table[2][6].replace('\nThank you for \nYour choice of\nGet 24/7 Acces\nFax credit car','')
            table[2][7] = table[2][7].replace('\nyour order with Winward. \n the finest floral products.\ns to Winward on www.winwardsilks.com\nd payments to Accounting 510-471-7326',
                                                '')
            df = pd.DataFrame(table[2])
            df = df.dropna()
            df['columns'] = df[0].str.split('\n').str[0]
            df['contents'] = df[0].str.split('\n').str[1:].str.join("").str.strip()
            df = df[['columns', 'contents']]
            test = df.set_index('columns')['contents'].to_frame().T.rename_axis(None, axis=1).copy()
            test = test.rename(columns=lambda x: x.strip())
            test.ORDERED = test.ORDERED.str.strip('-')[0]
            test.item_amount = test.AMOUNT.str.strip().str.split(' ----------')[0][0]
            test['SUBTOTAL'] = test.AMOUNT.str.strip().str.split(' ----------')[0][1].strip().split(' ')[0]
            test.SHIPPED = test.SHIPPED.str.split('-------     ')[0][0]
            test.item_amount = test.AMOUNT.str.split('-------     ')[0][0]
            test['TOTAL'] = test.AMOUNT.str.split('----------    ')[0][-1].replace(' ==========','')
            test['SURCHARGE'] = test['TOTAL'].astype(float) - test['SUBTOTAL'].astype(float)
            test['PRICE'] = test['PRICE'].str.strip('S')

            df2 = pd.DataFrame(table[0])
            df2 = df2.dropna()
            df2['columns'] = df2[0].str.split('\n').str[0]
            df2['contents'] = df2[0].str.split('\n').str[1:].str.join("").str.strip()
            df2 = df2[['columns', 'contents']]
            test2 = df2.set_index('columns')['contents'].to_frame().T.rename_axis(None, axis=1).copy()
            test2 = test2.rename(columns=lambda x: x.strip())

            text_extraction = current_page.extract_text().replace("""Thank you for your order with Winward. 
            Your choice of the finest floral products.
            Get 24/7 Access to Winward on www.winwardsilks.com""", '').split('\n')

            tracking_no = None
            invoice_no = None
            cus_prod = None
            extracted_surcharge = "0.00"

            for index, text in enumerate(text_extraction):
                if useRegex2(text) is not None:
                    #print(useRegex2(text).group())
                    invoice_no = useRegex2(text).group()
                if text.startswith('UPC: ') == False:
                    if useRegex(text) is not None:
                        tracking_no = useRegex(text).group()
                if text.startswith('SURCHARGE'):
                    extracted_surcharge = text.split(')')[1].strip()
                if useRegex3(text) is not None:
                    #print(text.lstrip('Cust Prod:'))
                    text = text.lstrip('Cust Prod: ')
                    cus_prod = text

            df3 = pd.DataFrame(table[1])

            df3 = df3.dropna()

            df3['columns'] = df3[0].str.split('\n').str[0]

            df3['contents'] = df3[0].str.split('\n').str[1:].str.join('').str.strip()

            df3 = df3.dropna()

            df3['columns'] = df3[0].str.split('\n').str[0]

            df3['contents'] = df3[0].str.split('\n').str[1:].str.join("").str.strip()
            df3 = df3[['columns', 'contents']]


            test3 = df3.set_index('columns')['contents'].to_frame().T.rename_axis(None, axis=1).copy()
            test3 = test3.rename(columns=lambda x: x.strip())

            trial = pd.concat([test,test2, test3], axis=1).reset_index()
            trial['Extracted Surcharge'] = extracted_surcharge
            trial['TRACKING'] = tracking_no
            trial['INVOICE_NO'] = invoice_no
            try:
                cus_prod = cus_prod.strip()
            except:
                cus_prod = None
            trial['CUS_PROD_NO'] = cus_prod
            
            if page_index == 0:
                main = trial[[
                    'ORDER  DATE',
                    'INVOICE DATE',
                    'DATE SHIPPED',
                    'PURCHASE ORDER NO.',
                    'SHIPPED',
                    'ITEM',
                    'TRACKING',
                    'INVOICE_NO',
                    'CUS_PROD_NO',
                    'PRICE',
                    'Extracted Surcharge',
                    'SURCHARGE',
                    'TOTAL',
                    'SUBTOTAL',
                    ]]
                main = split_surcharge(main)
            else:
                main2 = trial[[
                    'ORDER  DATE',
                    'INVOICE DATE',
                    'DATE SHIPPED',
                    'PURCHASE ORDER NO.',
                    'SHIPPED',
                    'ITEM',
                    'TRACKING',
                    'INVOICE_NO',
                    'CUS_PROD_NO',
                    'PRICE',
                    'Extracted Surcharge',
                    'SURCHARGE',
                    'TOTAL',
                    'SUBTOTAL',
                    ]].copy()
                main2 = split_surcharge(main2)

                main = pd.concat([
                    main,
                    main2])
                main['ORDER  DATE'] = pd.to_datetime(main['ORDER  DATE'].astype(str), infer_datetime_format=True)

                main['DATE SHIPPED'] = pd.to_datetime(main['DATE SHIPPED'].astype(str), infer_datetime_format=True)

                main['INVOICE DATE'] = pd.to_datetime(main['INVOICE DATE'].astype(str), infer_datetime_format=True)
    main = main[[                
                'ORDER  DATE',
                'INVOICE DATE',
                'DATE SHIPPED',
                'PURCHASE ORDER NO.',
                'SHIPPED',
                'ITEM',
                'TRACKING',
                'INVOICE_NO',
                'CUS_PROD_NO',
                'PRICE',]]
    main.columns = ['PO Date', 
                    'INVOICE Date',
                    'Expected Ship Date', 
                    'PO #', 
                    'Qty',
                    'Vendor SKU #', 
                    'TRACKING', 
                    'Invoice #',
                    'Customer Prod #',
                    'Rate'
                    ]

    main = main[['PO Date', 
                    'INVOICE Date',
                    'Expected Ship Date', 
                    'PO #', 
                    'Rate', 
                    'Qty',
                    'Vendor SKU #', 
                    'TRACKING', 
                    'Invoice #',
                    'Customer Prod #'       
                    ]]
    main.reset_index(drop=True, inplace=True)
    return main
