FROM apache/airflow:2.3.4

COPY package /package

RUN pip3 install boto3
RUN pip3 install pyspark
