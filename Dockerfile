
# Build the gobinary

FROM golang:1.23 AS gobuild

RUN update-ca-certificates

WORKDIR /go/src/app

COPY ./go.mod ./
COPY ./go.sum ./
RUN go mod download
RUN go mod verify

COPY ./ ./

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -v -ldflags "-s -w -linkmode external -extldflags -static" -o /go/bin/taskd ./cmds/taskd

FROM gcr.io/distroless/static

COPY --from=gobuild /go/bin/taskd /go/bin/taskd
EXPOSE 8080

ENTRYPOINT ["/go/bin/taskd"]
