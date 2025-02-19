FROM python:3.13-slim

WORKDIR /usr/src/app

COPY apps/publish-container/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/publish-container/src/ ./
CMD ["python", "./publish_container.py"]
