from __future__ import print_function
import pandas as pd
import gspread
import mysql.connector
import art
import warnings
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

warnings.filterwarnings('ignore')

def sql_data(Month): #Get data
    cnx = mysql.connector.connect(user = 'user', password = 'password', host = 'host', database = 'database')

    query = f"""SELECT uf_company_id, pay_voucher_date
    FROM dbportal.b_uts_crm_invoice as uts      
    left join dbportal.b_crm_invoice_basket as b on b.order_id = uts.value_id     
    left join dbportal.b_crm_invoice as i on i.id = b.order_id 
    where ((b.name like '%абон%обслуж%' or b.name like '%подписк%серви%') and b.name like '%{Month} 2022%') 
    and pay_voucher_date is not null"""

    data = pd.read_sql(query,cnx)

    x = data.shape[0]

    success_art = art.text2art('Success!', chr_ignore = True)
    failed_art = art.text2art('ErrorQ!', chr_ignore = True)

    print(art.text2art('Query:', chr_ignore = True))

    if x >= 2:
        print(success_art)
    if x < 2:
        print(failed_art)
    
    return data

def sql_data_sumi(Month): #Get data
    cnx = mysql.connector.connect(user = 'vitia', password = 'BYNWrsvMlx', host = '89.108.126.154', database = 'dbportal')

    query = f"""SELECT uf_company_id, i.price
    FROM dbportal.b_uts_crm_invoice as uts      
    left join dbportal.b_crm_invoice_basket as b on b.order_id = uts.value_id     
    left join dbportal.b_crm_invoice as i on i.id = b.order_id 
    where ((b.name like '%абон%обслуж%' or b.name like '%подписк%серви%') and b.name like '%{Month} 2022%') 
    """

    data = pd.read_sql(query,cnx)

    x = data.shape[0]

    success_art = art.text2art('Success!', chr_ignore = True)
    failed_art = art.text2art('ErrorQ!', chr_ignore = True)

    print(art.text2art('Query:', chr_ignore = True))

    if x >= 2:
        print(success_art)
    if x < 2:
        print(failed_art)
    
    return data

#                                                         <<< Connect to Google spreadsheet >>>
print(art.text2art('Connecting....')) 

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
SAMPLE_SPREADSHEET_ID = '1hohwM1_wByjp39xj_sVE-7bWpDy1tAJSPp7IG_S1z6s'
creds = ServiceAccountCredentials.from_json_keyfile_name(r'C:\Users\Admin\Desktop\Analitics\Курбат\Script/credentials.json', scope)
client = gspread.authorize(creds)

print(art.text2art('Success!')) 

sheet = client.open("Прогноз оплаты подписок").worksheet('Итоги') #Make a work spreadsheet
payed = client.open('Прогноз оплаты подписок').worksheet('Данные')
sum = client.open('Прогноз оплаты подписок').worksheet('Сумма по выставленным')
payed.clear()
sum.clear()

Month = sheet.acell('L1').value # Get value from cell K1 
data = sql_data(Month) # Get data from DB with value from cell 
sum_invoice = sql_data_sumi(Month) # Get info from DB about sum invoices

print(art.text2art('Waiting...'))

set_with_dataframe(payed, data) # Update spreadsheet
set_with_dataframe(sum, sum_invoice)

print(art.text2art('Done!'))