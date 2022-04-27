FROM python:3.9

WORKDIR /code
COPY app ./app
COPY main.py .
COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt