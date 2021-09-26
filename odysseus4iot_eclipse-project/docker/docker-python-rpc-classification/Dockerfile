FROM python:3
ADD rpc-classification.py /
RUN pip install pandas psycopg2 sklearn
EXPOSE 9001
CMD ["python","./rpc-classification.py","9001"]