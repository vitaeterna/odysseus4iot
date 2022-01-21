FROM python:3
ADD rpc_classification.py /
RUN pip install --upgrade pip
RUN pip install pandas psycopg2 scikit-learn==0.24.1
ENTRYPOINT ["python", "/rpc_classification.py"]