PACKAGES=$(shell go list ./...)

all: lint test

init: tools
	GO111MODULE=on go mod vendor

lint: init
	golangci-lint run ./...

test: init
	go test -race ./...

tools:
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(GOPATH)/bin v1.17.0

fmt: tools
	go fmt $(PACKAGES)

.PHONY: help lint test fmt tools
