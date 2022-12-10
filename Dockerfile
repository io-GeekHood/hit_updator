FROM golang:1.18.2-alpine3.16
RUN apk add build-base
ENV KAFKA_HOST=broker:29092
ENV KAFKA_TOPIC=raw_media
ENV MONGODB_URI=mongodb://test:test@172.27.226.107:27011
ENV TRY=10
RUN mkdir -p /home/app
ADD . /home/app/
RUN chmod 775 -R /home
WORKDIR /home/app
RUN go mod vendor
RUN GOOS=linux GO111MODULE=on go build -mod vendor -tags musl -ldflags="-w -s" -a -installsuffix cgo /home/app/cmd/main.go
RUN unset http_proxy https_proxy no_proxy HTTP_PROXY HTTPS_PROXY NO_PROXY
EXPOSE 8080
CMD ["/home/app/main"]