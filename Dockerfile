FROM python:3.9.4-slim-buster

WORKDIR /app
COPY ./requirements.txt /app/
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY api/ /app/api
COPY database/ /app/database
COPY src/ /app/src
