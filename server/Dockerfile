FROM python:3-alpine

COPY . /code

WORKDIR /code

RUN pip install --upgrade --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python3", "server.py"]