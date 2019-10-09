FROM binarycat/cx_oracle:5

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt
CMD ["python","-u","main/app.py"]
