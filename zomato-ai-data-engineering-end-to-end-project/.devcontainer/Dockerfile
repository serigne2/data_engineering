FROM mcr.microsoft.com/devcontainers/python:3.11

RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    snowflake-connector-python