FROM golang:1.19.1-alpine3.15 as builder
COPY ./ /go/src/grpc-get-events
WORKDIR /go/src/grpc-get-events
RUN go mod tidy
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /service main.go
CMD ["/service"]