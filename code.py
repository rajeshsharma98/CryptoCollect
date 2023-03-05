import requests
import time
import json
import pandas as pd
import ast
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from tqdm import trange
from Historic_Crypto import HistoricalData
from datetime import datetime, timedelta, date

'''
--------------------------------------------------------------------
 ETHERIUM -------------------------------------------------------------------
--------------------------------------------------------------------
'''
def etherium_scrap():
  ## This function scraps etherium data from website
  print('----------Etherium Data Scraping----------')
  # Column Names
  clmns = ['index','TxnHash', 'Method', 'Block', 'DateTime','Age', 'From', 'Flow', 'To', 'Value','TxnFee', 'GasPrice']
  # flow = in or out

  # Create a new dataframe to store the data
  df = pd.DataFrame(columns = clmns)

  # Scrap Etherium data
  for i in (trange(739)): 
    time.sleep(4) 
    site = "https://etherscan.io/txs?a=0x165CD37b4C644C2921454429E7F9358d18A45e14&ps=100&p="+str(i)
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = Request(site,headers=hdr)
    page = urlopen(req)
    soup = BeautifulSoup(page, "html.parser")
    table = soup.find('table', {'class':'table table-hover'}) 
    
    for row in table.findAll('tr')[1:]: 
      transaction = row.findAll('td') 
      entries = [td.text.strip() for td in transaction]

      # if page number passed have no entries
      if entries == ['There are no matching entries']:
        print('No more data available')
        break

      df.loc[len(df)] = entries 

  df = df.drop(['index'],axis=1) # drop first row -> index values

  # store data in csv format
  print('Storing etherium data to Dataset/etherium.csv........')
  df.to_csv('Dataset/etherium/etherium_reload.csv')
  return df

''' Etherium history function '''
def etherium():
  ## This function fetches etherium value( for etherium to dollar conversion) at the time of transaction and 
  ## stores those values in the  data frame as new columns
  print('----------Completing Etherium Data----------')

  ''' Read file'''
  df = pd.read_csv('Dataset/etherium/etherium.csv')
  # Create empty columns
  df['low'] = ' '
  df['high'] = ' '
  df['open'] = ' '
  df['close'] = ' '
  df['volume'] = ' '

  # Read each datetime and insert etherium cose at that point'''
  print('Fetching values........')
  for i in trange(df.shape[0]):
    # '2022-05-10 6:22:26' to '2022-05-10-6-22'
    start_date = df.loc[i,'DateTime']
    
    # convert to dateTime datatype
    start_date = datetime.strptime(start_date,'%Y-%m-%d %H:%M:%S')

    ''' Create end date '''
    end_date = start_date + timedelta(minutes=+1)
    # convert into required input format
    end_date = end_date.strftime("%Y-%m-%d-%H-%M")
    start_date = start_date.strftime("%Y-%m-%d-%H-%M")

    new = HistoricalData('ETH-USD',granularity=60,start_date=start_date,end_date=end_date, verbose=False).retrieve_data()
    # granuality means time interval after which to fetch the ether value
    # 86400 seconds = 24 hours  

    new.drop(new.tail(1).index,inplace=True) 
    ''' add 4 new columns '''
    df.loc[i,'low'] = new.iloc[0,0] # 0- low
    df.loc[i,'high'] = new.iloc[0,1] # 1- high
    df.loc[i,'open'] = new.iloc[0,2] # 2- open
    df.loc[i,'close'] = new.iloc[0,3] # 3- close
    df.loc[i,'volume'] = new.iloc[0,4] # 4- volume
  print('Storing etherium data to Dataset/All_etherium.csv........')
  df.to_csv('Dataset/etherium/all_etherium_reload.csv')
  return df

def chk_username(df):
  ## To check username that are not in normal format and put all of them in a list
  # general hash code of Etherium From address is of length 42
  lst = []
  for i in range(df.shape[0]):
    if len(df.From[i])<42:
      lst.append(df.From[i])
  print('No of address not in normal hash form: ',len(lst))  
  return lst

def cat_ether():
  ## Categorize tether users hash -> Coinbase 6 to Coinbase
  df = pd.read_csv('Dataset/etherium/all_etherium.csv')
  for i in trange(df.shape[0]):
    # 1. if adress length less than 42
    # 2. if last 4 characters are not "".eth"
    if ( len(df.From[i])<42 ) and ( df.From[i][-4:] != '.eth' ): 
      # 2.1. split word based on white space and get the first item of list and replace with actiual user name
      # example replace 'coinbase 1' with 'coinbase'
      df.loc[i,'From'] = df.loc[i,'From'].split()[0]
  df.to_csv('Dataset/etherium/all_etherium_categorized.csv')
  return df
 
def ether_topn(n):
  ## Get top (n) transactions of ether scan
  df = pd.read_csv('Dataset/etherium/all_etherium_categorized.csv')
  df.Value = df.Value.str.replace('[a-zA-Z]', '') # remove string
  df.Value = df.Value.str.replace(',','') # remove commas
  df.Value = df.Value.astype('float') # convert datatype of Value column
  df.TxnFee = df.TxnFee.astype('float') # convert datatype of TxnFee column

  # convert ehterium to dollar
  ## Value - The amount sent in the transaction.    : given in Ether
  ## Transaction Fee - The fee paid for making the transaction.   : given in Ether
  df['Total'] = df.Value+df.TxnFee # Value + Transaction = total amount
  df['Total'] = df['Total'] * df.open # Ether value to Dollar 

  # Sort all dataset according to 'Total' column
  df = df.sort_values(by='Total', ascending=False)
  # get top (n) columns
  df = df.head(n)
  return df
 
'''
--------------------------------------------------------------------
 BITCOIN --------------------------------------------------------------------
--------------------------------------------------------------------
'''
def btc():
  ## fetch btc data
  print('---------- Bitcoin Data Fetching ----------')
  address_btc = '357a3So9CbsNfBBgFYACGvxxS6tMaDoa1P'
  offset_value = '0'
  url = 'https://blockchain.info/rawaddr/'+address_btc+'?offset='+offset_value # offset cross verifies -> working fine
  # print('Fettching response from API..........')
  output = urlopen(url) # store output from the url
  
  # Reading HTTPresponse data
  data_json = json.loads(output.read())
  # print('HTTPresponse to data..........')

  clmn = data_json['txs'][0].keys()
  df = pd.DataFrame(columns = clmn)
  time.sleep(20)

  # round of to nearest hundread and pass this in for loop range
  total_trans = data_json['n_tx']
  headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)\
              AppleWebKit/537.36 (KHTML, like Gecko) Cafari/537.36'}

  # for now 18400 entries so hard code to that -> later have to use 'total_trans' variable
  for i in trange(-100,18400,100):
    offset_value = str(100+i)
    url = 'https://blockchain.info/rawaddr/'+address_btc+'?offset='+offset_value
    req = Request(url,headers=headers)

    # print('Fettching response from API..........')
    output = urlopen(req) # store output from the url

    # Reading HTTPresponse data
    data_json = json.loads(output.read())
    time.sleep(12)
    df = df.append(data_json['txs'], ignore_index=True)

    print('Storing bitcoin data to Dataset/bitc.csv........')
    df.to_csv('Dataset/btc/btc_reload.csv')
  return df

def btc_clean():
  # output column dictionary as seperate columns
  df = pd.read_csv('../notebook/btc_reload.csv') 
  
  # Create new columns for Out column dictionary
  dict_output = pd.DataFrame()
  print('Output columns--------')
  for i in trange(df.shape[0]):
    s = ast.literal_eval(df.out[i])
    # gives information of multiple output
    condi = pd.DataFrame.from_dict(s)
    condi = condi[condi['addr'] == '357a3So9CbsNfBBgFYACGvxxS6tMaDoa1P']
    condi['hash'] = df.loc[i,'hash']
    dict_output = pd.concat([dict_output, condi])
  dict_output.to_csv('Dataset/btc/btc_out.csv')
  merge = dict_output.merge(df, left_on='hash', right_on='hash')  
  merge.to_csv('Dataset/btc/btc_merge.csv')
  return merge

def btc_time():
  ## convert unix time to normal time stamp
  df = pd.read_csv('../notebook/btc_reload.csv') 
  df['DateTime'] = ''
  for i in trange(df.shape[0]):
    df.loc[i,'DateTime'] = datetime.fromtimestamp(df.time[i])
  df.to_csv('Dataset/btc_cleaned.csv')
  return df

def bitcoin():
  ## merge hostorical and fetched data of bitcoin

  df = pd.read_csv('../notebook/btc_cleaned.csv')
  for i in trange(df.shape[0]):
    df.loc[i,'DateTime'] = df.loc[i,'DateTime'][:10]

  history = pd.read_csv('Dataset/btc/tether_usd.csv')

  merge = df.merge(history, left_on='timestamp', right_on='Date')
  merge.to_csv('Dataset/btc/all_bitcoin.csv')
  return merge

'''
--------------------------------------------------------------------
 TETHER --------------------------------------------------------------------
--------------------------------------------------------------------
'''

## Tether.csv -> file directly downloaded from the website with filter == 'View transaction received'

def tether(): 
  ## merge historical and fetched data of tether
  df = pd.read_csv('Dataset/tether/tether.csv')
  df['time'] = df['timestamp']
  # thought: can do operations on columns without for loop
  # df['timestamp'].replace('/','') 
  for i in trange(df.shape[0]):
    df.loc[i,'timestamp'] = df.loc[i,'timestamp'][:10]
    # change '2022/05/02' = '2022-05-02' to make it equal with date format in historical data   
    df.loc[i,'timestamp'] = df['timestamp'][i].replace('/','-')

  history = pd.read_csv('Dataset/tether/tether_usd.csv')

  merge = df.merge(history, left_on='timestamp', right_on='Date')
  merge.to_csv('Dataset/tether/all_tether.csv')
  return merge


def tether_groupbyTime():
  ## Group by time to count all the transaction between a particualr time interval accordingly
  df = pd.read_csv('Dataset/tether/tether.csv')
  # convert string type date to datetime datatype    
  for i in trange(df.shape[0]):
    df.loc[i,'timestamp'] = datetime.strptime(df.loc[i,'timestamp'][:10],'%Y/%m/%d')
  return df.groupby(pd.Grouper(key='timestamp', freq='5d')).count() # each row item count of that column for that particular time

def tether_collect():
  ## to collect data using API- TronScan-frontend

  df = pd.read_csv('Dataset/tether/tether.csv')
  # df['usdt'] = ''
  # df['icon_url'] = '' 
  # df['symbol'] = '' 
  # df['level'] = ''
  # df['decimals'] = '' 
  # df['name'] = '' 
  # df['to_address'] = '' 
  # df['contract_address'] = ''
  # df['type'] = '' 
  # df['vip'] = '' 
  # df['tokenType'] = '' 
  # df['from_address'] = '' 
  # df['amount_str'] = ''

  # Create new empty columns
  columns = ['icon_url', 'symbol', 'level', 'decimals', 'name', 'to_address', 'contract_address', 'type', 'vip', 'tokenType', 'from_address', 'amount_str']
  for z in columns:
    df[z]=''

  for i in trange(df.shape[0]):
    hash = df.loc[i,'hash']
    url = 'https://apilist.tronscan.org/api/transaction-info?hash='+hash
    output = urlopen(url)
    data_json = json.loads(output.read())
    if 'tokenTransferInfo' in data_json:
      for j in columns:
        df.loc[i,j] = data_json['tokenTransferInfo'][j]
    if i%4000 == 0:
      time.sleep(200)
  df.to_csv('Dataset/tether/updated_all_tether.csv')
  return df

def tether_new():
  ## merge historical and fetched data of tether -> USDT and TRX both using previously processed data
  ## take the latest created file updated_all_tether.csv
  ## Delete previously fetched historical data if present
  df = pd.read_csv('Dataset/tether/updated_all_tether.csv')
  history_trx = pd.read_csv('Dataset/tether/tether_trx_usd.csv')
  history_usdt = pd.read_csv('Dataset/tether/tether_usdt_usd.csv')
  merged_both = history_trx.merge(history_usdt, left_on='Date_trx', right_on='Date_usdt')
  df.drop(columns = ['Open','High','Low','Close','Adj Close','Volume'],inplace=True)
  merged_all = merged_both.merge(df, left_on='Date_trx', right_on='timestamp')
  merged_all.to_csv('Dataset/tether/merged_trx_usdt.csv')
  return merged_all

def tether_data_clean():
  # for updated_all_tether file
  df = pd.read_csv('Dataset/tether/updated_all_tether.csv')
  df = df.iloc[: , 1:]
  df = df.drop(['ownerAddress'],axis=1)
  df.rename(columns={"amount": "trx_token_amount"},inplace=True)

  # for merged_trx_updated file -> if above code lines not executed
  df = pd.read_csv('Dataset/tether/merged_trx_usdt.csv')
  df = df.iloc[: , 1:]
  df = df.drop(['ownerAddress'],axis=1)
  df.rename(columns={"amount": "trx_token_amount"},inplace=True)
  df = df.drop(['Date_usdt','Date_trx','Date','timestamp'],axis=1)
  df.rename(columns={"time": "timestamp"},inplace=True)
  df.to_csv('Dataset/tether/merged_trx_usdt.csv')
  return df
 
''' 
--------------------------------------------------------------------
Other Functions -------------------------------------------------------------
--------------------------------------------------------------------
'''
def date_unix(): 
  # date(year, month, date)
  # dat = datetime.date(2021, 8, 6)
  dat = date(2021, 8, 6)
  return(time.mktime(dat.timetuple()))

''' 
--------------------------------------------------------------------
Function calls -------------------------------------------------------------
--------------------------------------------------------------------
'''

# a = etherium()
# b = etherium_scrap()
# h = chk_username()
# i = cat_ether()
# j = ether_topn()

# c = btc()
# d = btc_clean()
# e = btc_time()
# f = bitcoin()

# g = tether()


