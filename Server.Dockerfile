FROM python:3.9-alpine

WORKDIR /app

RUN apk add --no-cache nodejs npm
RUN npm install -g nodemon
