FROM python:3.9-slim-buster
WORKDIR /app
COPY requirements.txt requirements.txt
ENV MONGODB_URI=mongodb://test:test@172.27.226.107:27011
ENV IMG_BUCK_NAME=okala-images-main
ENV REF_BUCK_NAME=okala-refrence-main
RUN python3.9 -m pip install --upgrade pip
RUN python3.9 -m pip install -r requirements.txt
RUN mkidir refrences
COPY . /app/
EXPOSE 27017
ENTRYPOINT ["python3.9", "producer.py"]
