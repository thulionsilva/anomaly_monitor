FROM python:3.11.6-alpine

WORKDIR /src

COPY ./requirements.txt ./requirements.txt
COPY train.py predict.py preprocessing2.py .

RUN pip3 install --upgrade pip

RUN apk add --no-cache python3-dev libpq-dev bash

RUN pip install -r ./requirements.txt

# Install crontab
RUN apk update && apk add --no-cache dcron

COPY ./entrypoint.sh /
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
