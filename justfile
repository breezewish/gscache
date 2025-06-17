default: build

build:
    CGO_ENABLED=0 go build -o bin/gscache ./cmd/gscache

test: test-unit test-e2e

test-unit:
    go test ./...

test-e2e:
    go test ./tests/...

release:
    goreleaser release --clean
