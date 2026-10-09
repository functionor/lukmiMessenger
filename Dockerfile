FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/messaging-service ./cmd/server
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/messaging-service /messaging-service
USER nonroot:nonroot
ENTRYPOINT ["/messaging-service"]
