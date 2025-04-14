FROM pycaret/slim

USER root

WORKDIR /src

COPY ./requirements.txt ./requirements.txt
COPY train.py predict.py pre_processing.py rewind.py .env .

RUN apt-get update && apt-get install -y cron

RUN pip3 install --upgrade pip

RUN pip install -r ./requirements.txt

COPY ./entrypoint.sh /
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
