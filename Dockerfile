# Build the mails
FROM node:20 as mailbuild

WORKDIR /app/mails

COPY mails/package.json mails/pnpm-lock.yaml ./
RUN npx pnpm install

COPY ./mails .
RUN npm run build

# Build the go-binary
FROM golang:1.23 AS gobuild

RUN update-ca-certificates

WORKDIR /go/src/app

COPY ./go.mod ./
COPY ./go.sum ./
RUN go mod download
RUN go mod verify

COPY ./ ./
COPY --from=mailbuild /app/mails/dist /go/src/app/cmds/taskd/mails

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -v -ldflags "-s -w -linkmode external -extldflags -static" -o /go/bin/taskd ./cmds/taskd

# Build the final image
FROM gcr.io/distroless/static

COPY --from=gobuild /go/bin/taskd /go/bin/taskd
EXPOSE 8080

ENTRYPOINT ["/go/bin/taskd"]
