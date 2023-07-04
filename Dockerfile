FROM apache/airflow:2.6.1
COPY requirement.txt .
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r ./requirement.txt
