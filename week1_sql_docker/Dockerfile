FROM python:3.9.1
WORKDIR /app
COPY requirements.txt requirements.txt
COPY ingest_ny_data.py ingest_ny_data.py
RUN pip install -r requirements.txt
ENTRYPOINT [ "python", "ingest_ny_data.py"]
