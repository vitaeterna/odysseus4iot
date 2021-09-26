FROM python:3
ADD rpc_classification.py /
RUN pip install pandas psycopg2 sklearn
ENTRYPOINT ["python","/rpc_classification.py"]