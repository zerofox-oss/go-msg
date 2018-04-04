PACKAGES=$(shell go list ./...)

all: lint test

go-lint:
	go get -u github.com/alecthomas/gometalinter
	gometalinter --install --force

lint: go-lint
	gometalinter \
		--disable-all \
		--enable=gofmt \
		--enable=goimports \
		--enable=golint \
		--enable=misspell \
		--enable=vetshadow \
		--tests \
		./...

test:
	go test -race -v ./...

fmt: tools
	go fmt $(PACKAGES)

.PHONY: help lint test fmt tools
