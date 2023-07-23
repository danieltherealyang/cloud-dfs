FROM python:3.11-alpine

WORKDIR /app

RUN apk add --no-cache nodejs npm
RUN npm install -g nodemon
RUN pip install mysql-connector-python