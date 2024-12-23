FROM python:3.12.7-slim

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/*


WORKDIR /code

COPY requirements.txt .

RUN pip install -U pip wheel && pip install -r requirements.txt

COPY app/ app/

CMD ["python3", "-m", "app"]
