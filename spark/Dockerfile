FROM gradiant/spark:2.4.0-python

COPY . /code

WORKDIR /code

RUN pip install --upgrade --no-cache-dir -r requirements.txt

CMD ["python3", "app.py"]