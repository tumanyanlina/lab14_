FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY collector/ .
RUN go mod tidy && go build -o mfc-collector .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/mfc-collector .
RUN mkdir -p /data
CMD ["./mfc-collector"]