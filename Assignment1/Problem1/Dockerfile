FROM python
RUN pip install boto
RUN pip install boto3
RUN pip install pandas
RUN pip install beautifulsoup4
Run pip install lxml
RUN pip install html5lib
RUN pip install urllib3
ADD Problem1.py /
ADD Data.txt /
CMD ["python","./Problem1.py","$(cat Data.txt)"]