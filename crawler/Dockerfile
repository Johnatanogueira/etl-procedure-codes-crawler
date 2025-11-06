# https://airflow.apache.org/docs/docker-stack/build.html
FROM python:3.12.4-slim AS base

ENV CHROME_VERSION="127.0.6533.99"
ENV CHROME_URL="https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chrome-linux64.zip"
ENV CHROME_DRIVER_URL="https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chromedriver-linux64.zip"

RUN mkdir -p /root/.aws # -p for no error if directory already exists
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        vim \
        tzdata \
        wget \
        unzip 
  
# Chrome Dependencies
RUN apt-get update && apt-get install -y libasound2 \
  libnss3 \
  libglib2.0-0 \
  libgconf-2-4 \
  libfontconfig1 \
  libgtk-3-0 \
  libgbm1

# Chrome for testing Install
RUN set -eux; \
wget -O chrome-linux64.zip https://storage.googleapis.com/chrome-for-testing-public/123.0.6312.58/linux64/chrome-linux64.zip && \
unzip chrome-linux64.zip && \
mv chrome-linux64/* /usr/local/bin/ && \
chmod +x /usr/local/bin/chrome && \
rm -rf chrome-linux64.zip chrome-linux64

# ChromeDriver Install
RUN set -eux; \
    wget -O chromedriver-linux64.zip https://storage.googleapis.com/chrome-for-testing-public/123.0.6312.58/linux64/chromedriver-linux64.zip && \
    unzip chromedriver-linux64.zip && \
    mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf chromedriver-linux64.zip chromedriver-linux64

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

FROM base

COPY . /app
