FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN GOTOOLCHAIN=local go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/messaging-service ./cmd/server

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/messaging-service /messaging-service
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/messaging-service"]
