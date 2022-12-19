FROM python:3.9-slim-buster
WORKDIR /app
COPY requirements.txt requirements.txt
ENV MONGODB_URI=mongodb://test:test@test-mongotest-1:27017
ENV MONGODB_DATABASE=okala-fmcg-all
ENV MINIO_HOST=minio-nginx-1:9000
ENV MINIO_USER=minioadmin
ENV MINIO_PASSWORD=sghllkfij,dhvrndld
ENV MONGODB_COLLECTION=products
ENV INGESTION_API=http://127.0.0.1:8080/api/v1/gateway
ENV IMG_BUCK_NAME=okala-images-all
ENV REF_BUCK_NAME=okala-refrence-all
RUN python3.9 -m pip install --upgrade pip
RUN python3.9 -m pip install -r requirements.txt
RUN mkdir refrences
RUN mkdir state_storage
COPY . /app/
EXPOSE 27017
ENTRYPOINT ["python3.9", "producer.py"]
