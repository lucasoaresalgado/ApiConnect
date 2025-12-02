FROM python:3.13.9-alpine3.22

# Install necessary packages
RUN pip install --no-cache-dir "fastapi[standard]" "uvicorn[standard]" boto3 aiohttp python-dotenv pandas pyarrow

EXPOSE 8000

WORKDIR /ApiExtract

COPY . .

ENTRYPOINT ["python", "src/apiEndpoint.py"]