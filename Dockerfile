FROM apache/airflow:2.3.4

COPY package /package

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt