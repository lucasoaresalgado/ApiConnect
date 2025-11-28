FROM python:3.13.9-alpine3.22

RUN pip install "fastapi[standard]"
RUN pip install "uvicorn[standard]"
RUN pip install boto3
RUN pip install aiohttp
RUN pip install python-dotenv
RUN pip install pandas

EXPOSE 8000

WORKDIR /ApiExtract

COPY . .

ENTRYPOINT ["python", "src/apiEndpoint.py"]