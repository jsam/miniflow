FROM golang:alpine

COPY . /go/src/github.com/jsam/miniflow
WORKDIR /go/src/github.com/jsam/miniflow

RUN CGO_ENABLED=0 go build -a -installsuffix cgo -o miniflow .

ENTRYPOINT ["./miniflow"]
