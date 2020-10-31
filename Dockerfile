FROM python:3.9

WORKDIR /usr/src/app

COPY . .

RUN pip install -r requirements_dev.txt
