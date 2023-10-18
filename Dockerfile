FROM python:3.9.1

WORKDIR /app

COPY pipeline.py pipeline.py

RUN pip install pandas sqlalchemy psycopg2

ENTRYPOINT [ "python", "pipeline.py" ]