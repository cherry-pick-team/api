FROM python:3.5.2-slim

WORKDIR /usr/src/app
ENV PYTHONPATH=/usr/src/app

COPY ./requirements.txt /usr/src/app/

RUN apt-get update \
     && apt-get install -y netcat libpq-dev gcc \
     && pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

CMD ./wait.sh && python cherry-pick-api.py
