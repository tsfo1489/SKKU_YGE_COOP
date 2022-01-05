FROM tensorflow/tensorflow:2.7.0-gpu-jupyter

WORKDIR /app

COPY requirements.txt /app/

RUN pip install -r requirements.txt

EXPOSE 8000

COPY ./django /app/