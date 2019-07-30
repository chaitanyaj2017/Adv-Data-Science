
import pandas as pd
import urllib3
from bs4 import BeautifulSoup
#import numpy as np
import io
import zipfile
import logging
import os
import sys
import glob
import warnings
import boto
from boto.s3.key import Key
warnings.filterwarnings('ignore')


root=logging.getLogger()
root.setLevel(logging.DEBUG)


ch1=logging.FileHandler('app.log')
ch1.setLevel(logging.DEBUG)
formatter=logging.Formatter('%(asctime)s-%(levelname)s-%(message)s')
ch1.setFormatter(formatter)
root.addHandler(ch1)


ch2=logging.StreamHandler(sys.stdout)
ch2.setLevel(logging.DEBUG)
formatter1=logging.Formatter('%(levelname)s-%(message)s')
ch2.setFormatter(formatter1)
root.addHandler(ch2)

url='https://www.sec.gov/data/edgar-log-file-data-set.html'


flag=False
if len(sys.argv)>1:
    for i in range(len(sys.argv)):
        val=str(sys.argv[i])
        if val.startswith('year='):
            root.info('Setting value from argument')
            year=val.split('=')[1]
            if int(year)>2002 and int(year)<2018:
                flag=True
        elif val.startswith('aws_access_key_id='):
            aws_access_key = val.split('=')[1]
        elif val.startswith('aws_secret_access_key='):
            aws_secret_access_key = val.split('=')[1]
        elif val.startswith('adsBucket='):
            bucketName = val.split('=')[1]
        elif val.startswith('region='):
            region = val.split('=')[1]

if flag==False:
    root.info('Setting default value')
    year='2003'
    cred = pd.read_csv('C:\cjfiles\ADS_Srikanth\credentials.csv')
    aws_access_key = cred.loc[0, 'Access key ID']
    aws_secret_access_key = cred.loc[0, 'Secret access key']
    bucketName = 'ads-assignment1'
    region = 'us-east-1'
day='01'


http=urllib3.PoolManager()
response=http.request('GET',url)
bet=BeautifulSoup(response.data,'html.parser')
#bet.prettify()


lst=bet.find_all('a') #,text='EDGAR Log File Data Set FAQs'
for i in range(1,len(lst)):
    if lst[i].text=='Logfile List':
        url2=lst[i].attrs['href']
        break
url2

url3='https://www.sec.gov'+url2
url3

http=urllib3.PoolManager()
response=http.request('GET',url3)
b=BeautifulSoup(response.data,'html.parser')

lstlinks=b.prettify().split('\n')

o=lstlinks[10]
o1=o.replace('\r','').rstrip()
#o1

p=[]
for l in lstlinks:
    if l.startswith('www'):
        p.append(l.replace('\r','').rstrip())
#p


url4='https://'+p[10][:-25]

d={'01':'Qtr1','02':'Qtr1','03':'Qtr1','04':'Qtr2','05':'Qtr2','06':'Qtr2','07':'Qtr3','08':'Qtr3','09':'Qtr3','10':'Qtr4','11':'Qtr4','12':'Qtr4'}
#d.items()


final=[]
for key,value in d.items():
    final.append(url4+year+'/'+value+'/log'+year+key+day+'.zip')
#final


final[0]
len(final)


files=[]
cwd: str=os.getcwd()
p=cwd+'/'+ 'logfiles_'+year+day+'/'
if not os.path.exists(p):
    os.makedirs(p)
flag2=False
if __name__=='__main__':
    flag2=True
for i in range(0,len(final)):
    if flag2:
        http=urllib3.PoolManager()
        response=http.request('GET',final[i])
        z = zipfile.ZipFile(io.BytesIO(response.data))
        z.extractall(path=p)

fileList=glob.glob(p+'*.csv')
#print(fileList)


dfs=[]
for file in fileList:
    dfs.append(pd.read_csv(file))

for i,df in enumerate(dfs):
    # cols=['ip','date','time','zone','cik','accession','extention','code','size','idx','norefer','noagent','find','crawler','browser']
    df.dropna(subset=['idx'], inplace=True)
    df.dropna(subset=['zone'], inplace=True)
    df.dropna(subset=['cik'], inplace=True)
    df.dropna(subset=['accession'], inplace=True)
    df.dropna(subset=['date'], inplace=True)
    root.info('dropping null value rows for dataframe - %d'%(i+1))

    if 'crawler' in df.columns:
        rows = df['crawler'].apply(lambda x: x not in [0.0, 1.0]).sum()
        root.info('The no of invalid values in crawler in dataframe - %d is %d'%(i+1,rows))
    if 'size' in df.columns:
        rows = df['size'].apply(lambda x: x not in [0.0, 1.0]).sum()
        root.info('The no of invalid values in size in dataframe - %d is %d' % (i + 1, rows))
    if 'norefer' in df.columns:
        rows = df['norefer'].apply(lambda x: x not in [0.0, 1.0]).sum()
        root.info('The no of invalid values in norefer in dataframe - %d is %d' % (i + 1, rows))
    if 'noagent' in df.columns:
        rows = df['noagent'].apply(lambda x: x not in [0.0, 1.0]).sum()
        root.info('The no of invalid values in noagent in dataframe - %d is %d' % (i + 1, rows))
    if 'find' in df.columns:
        rows = df['find'].apply(lambda x: x not in [0.0, 1.0]).sum()
        root.info('The no of invalid values in find in dataframe - %d is %d' % (i + 1, rows))
    if df['browser'].isnull().sum()>0:
        if 'browser' in df.columns:
            df['browser'].value_counts()
            v = df['browser'].value_counts().index[0]
            df['browser'].fillna(value=v, inplace=True)
            root.info('Null values replaced from browser column in dataframe - %d'%(i+1))
    if df['size'].isnull().sum() > 0:
        if 'size' in df.columns:
            v = df['size'].value_counts().index[0]
            df['size'].fillna(value=v, inplace=True)
            root.info('Null values replaced from size column in dataframe - %d' % (i + 1))

root.info('Concat all dataframe in a single one')
master_df = pd.concat(dfs)
root.info('Saving dataframe to file')
master_df.to_csv('masterfile.csv')

fz='master.zip'
root.info('Creating zip file....')
if __name__=='__main__':
    with zipfile.ZipFile(file=fz,mode='w') as zipObj:
        zipObj.write('app.log')
        zipObj.write('masterfile.csv')
        fn=zipObj.filename

root.info('Uploading file to s3 bucket....')

try:
    if region not in ['us-east-1','us-east-2','us-west-1','us-west-2','us-gov-east-1''us-gov-west-1']:
        exit()
    conn=boto.s3.connect_to_region('us-east-1',aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_access_key,is_secure=True)
    bucket=conn.get_bucket(bucket_name=bucketName)
    fk=Key(bucket)
    fk.name=fn
    fk.set_contents_from_filename(fz)
    conn.close()
    root.info('File uploaded successfully to s3')
except Exception as e:
    root.error('Some problem with connecting to s3')

