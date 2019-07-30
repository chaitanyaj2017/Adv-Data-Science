import pandas as pd
import numpy as np
#from urllib import request
import logging
import sys
from bs4 import BeautifulSoup
from zipfile import ZipFile
import urllib3
import os
import boto3
from boto.s3.key import Key
import boto.s3.connection


#logging
root=logging.getLogger()
root.setLevel(logging.DEBUG)

ch1=logging.FileHandler('app.log')
ch1.setLevel(logging.DEBUG)
formatter=logging.Formatter('%(asctime)s-%(levelname)s-%(message)s')
ch1.setFormatter(formatter)
root.addHandler(ch1)


ch=logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter1=logging.Formatter('%(levelname)s-%(message)s')
ch.setFormatter(formatter1)
root.addHandler(ch)


flag=False
#cik=None
#accno=None
#aws_access_key=None
#aws_secret_access_key=None
#bucketName=None


if len(sys.argv)>3:
    flag=True
    root.info('Setting parameters from file')
    for i in range(1,len(sys.argv)):
        val=str(sys.argv[i])
        if val.startswith('cik='):
            cikno=val.split('=')[1].lstrip('0')
        elif val.startswith('accno='):
            accno=val.split('=')[1]
        elif val.startswith('aws_access_key_id='):
            aws_access_key=val.split('=')[1]
        elif val.startswith('aws_secret_access_key='):
            aws_secret_access_key=val.split('=')[1]
        elif val.startswith('adsBucket='):
            bucketName=val.split('=')[1]
        elif val.startswith('region='):
            region=val.split('=')[1]
        


# In[7]:


if flag==False:
    root.info('Setting default parameter values')
    cikno='51143'
    accno='000005114313000007'
    cred=pd.read_csv('C:\cjfiles\ADS_Srikanth\credentials.csv')
    #cred.head()
    aws_access_key=cred.loc[0,'Access key ID']
    aws_secret_access_key=cred.loc[0,'Secret access key']
    bucketName='ads-assignment1'
    region='us-east-1'
    


# In[8]:


url='http://www.sec.gov/Archives/edgar/data/'+cikno+'/'+accno
print(url)
http=urllib3.PoolManager()
html=http.request('GET',url)
#with request.urlopen(url) as response:
#    html=response.read()
#html

soup=BeautifulSoup(html.data,'html.parser')
#print(soup.prettify())


soup.find_all('a')


url2='http://www.sec.gov/Archives/edgar/data/'+cikno+'/'+accno+'/'+accno[0:10]+'-'+accno[10:12]+'-'+accno[12:19]+'-index.html'


print(url2)

http=urllib3.PoolManager()
h=http.request('GET',url2)
#h


from bs4 import BeautifulSoup
bs=BeautifulSoup(h.data,'html.parser')
#bs.prettify()


bs.find_all('table')
bs.find_all('table',attrs={'summary':'Document Format Files'})

#from lxml import html
#import requests
#page=requests.get(url2)
#x=html.fromstring(page.content)


#a=bs.find_all('tr')[1]
#a.findall


n=bs.find('td',text='10-Q')
l=n.fetchNextSiblings()[0]
k=l.find('a').attrs['href']
k

url3='https://www.sec.gov'+k
print(url3)

dhtm=pd.read_html(url3)
#dhtm

http=urllib3.PoolManager()
response=http.request('GET',url3)
bes=BeautifulSoup(response.data,'html.parser')
#bes.prettify()

tables=bes.findAll('table')

reqidx=[]
rds=None
for j in range(0,len(tables)):
    rds=tables[j].findAll('td')
    for i in range(0,len(rds)):
        if 'background:' in str(rds[i]):
            reqidx.append(j)
            break


reqidx[:2]
type(dhtm[34])

type(dhtm)
dhtm[34][0:1][0].notnull()
#reqidx

for k in reqidx:
    rowstart=0
    for rows in range(0,len(dhtm[k])-1):
            if dhtm[k][rows:rows+1][0].notnull()[rows]:
                rowstart=rows
                break
    s=[]
    for j in range(rowstart,len(dhtm[k])-1):
        s.append(dhtm[k].iloc[j].dropna().tolist())
        for i in range(0,len(s)):
            if '$' in s[i]:
                s[i].remove('$')
    a=pd.DataFrame(s)
    cwd=os.getcwd()
    if not os.path.exists(cwd+'/'+accno):
        os.makedirs(cwd+'/'+accno)
    a.to_csv(cwd+'/'+accno+'/Table'+str(k+1)+'.csv',sep=',',header=False,index=False)    

s[0:2]
zf=accno+'.zip'
if __name__=='__main__':
    with ZipFile(zf,'w') as zipObj:
        zipObj.write('app.log')
        for folderName,subFolders,fileNames in os.walk(cwd+'/'+accno+'/'):
            for filename in fileNames:
                filepath=os.path.join(folderName,filename)
                zipObj.write(filepath)
            fn = zipObj.filename


try:
    if region not in ['us-east-1','us-east-2','us-west-1','us-west-2','us-gov-east-1''us-gov-west-1']:
        exit()
    conn=boto.s3.connect_to_region('us-east-1',aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_access_key,is_secure=True,calling_format=boto.s3.connection.OrdinaryCallingFormat())
    bucket=conn.get_bucket(bucket_name=bucketName)
    fp=os.getcwd()+'/'+zf
    fk = Key(bucket)
    fk.name = fn
    fk.set_contents_from_filename(zf)
except:
    root.error('Some problem with connecting to AWS s3')




