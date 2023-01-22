FROM golang:1.18.2-alpine3.16
RUN apk add build-base
ENV BROKER_HOST=broker:29092
ENV KAFKA_TOPIC_LISTEN_UPDATOR=done_media_create
ENV AWS_HOST=nginx:9000
ENV AWS_ACCESS=minioadmin
ENV AWS_SECRET=sghllkfij,dhvrndld
ENV MONGODB_URI=mongodb://hit_admin:*5up3r53CUR3D@mongodb:27017
ENV GOPROXY=https://goproxy.cn
RUN mkdir -p /home/app
ADD . /home/app/
RUN chmod 775 -R /home
WORKDIR /home/app
RUN go mod vendor
RUN GOOS=linux GO111MODULE=on go build -mod vendor -tags musl -ldflags="-w -s" -a -installsuffix cgo /home/app/cmd/main.go
CMD ["/home/app/main"]
