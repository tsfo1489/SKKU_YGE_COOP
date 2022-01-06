FROM pytorch/pytorch:1.10.0-cuda11.3-cudnn8-devel

WORKDIR /app

COPY requirements.txt /app/

RUN pip install -r requirements.txt

EXPOSE 8000

COPY ./web /app/
