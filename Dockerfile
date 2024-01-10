FROM golang:latest

# Set the Current Working Directory inside the container
WORKDIR /app/pgx_multiload

# We want to populate the module cache based on the go.{mod,sum} files.
COPY go.mod .
COPY go.sum .

RUN go mod download
COPY . .

# Build the Go app
RUN go build -o ./out/pgx_multiload .


# Run the binary program produced by `go install`
CMD ["./out/pgx_multiload"]