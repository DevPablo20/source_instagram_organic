FROM python:3.11-slim

WORKDIR /airbyte/integration_code

COPY setup.py ./
COPY main.py ./
COPY source_instagram_organic ./source_instagram_organic

RUN pip install --no-cache-dir .

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=devpablo20/source-instagram-organic
