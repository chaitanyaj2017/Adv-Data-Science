FROM python

#Adding required libraries from file

ADD requirements.txt /
RUN pip install -r requirements.txt

#Create a dir where all files will be saved
RUN mkdir "AllFiles"
RUN cd "AllFiles"



ADD luigiworkflow.py /

#Running luigi local-scheduler
CMD ["luigid","&"]

#Provide default values in case parameters are not given
#Default Amazon keys are invalid
CMD [ "python", "./luigiworkflow.py","upload2S3 ","--local-scheduler","--accessKey","AGLHROWE24DGHRA","--secretAccessKey","KYoGxxGRTYUSDG4924HDGRHC"]