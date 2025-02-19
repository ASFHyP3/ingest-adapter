FROM python:3.13-slim

WORKDIR /usr/src/app

COPY plugin/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY plugin/src/ ./
ENTRYPOINT ["python", "./plugin.py"]
