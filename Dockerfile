FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /ingest .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /ingest /usr/local/bin/ingest
ENTRYPOINT ["ingest"]
