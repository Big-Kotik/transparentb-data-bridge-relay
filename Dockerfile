FROM golang:1.20

WORKDIR /data_bridge
COPY . .
RUN go mod download
RUN go mod tidy
RUN go build -o relay cmd/relay-server/main.go

CMD ./relay
