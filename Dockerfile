FROM golang:latest as builder

WORKDIR /go-modules

COPY . ./

# Building using -mod=vendor, which will utilize the v
#RUN go get
#RUN go mod vendor
RUN CGO_ENABLED=0 GOOS=linux go build -v -mod=vendor -o postgres-insert-gridcell

#FROM alpine:3.8
FROM scratch

WORKDIR /root/

COPY --from=builder /go-modules/postgres-insert-gridcell .
COPY conf.json .

CMD ["./postgres-insert-gridcell"]