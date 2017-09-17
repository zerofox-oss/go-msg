GOTOOLS = \
	github.com/golang/lint/golint \
	golang.org/x/tools/cmd/goimports

PACKAGES=$(shell go list ./...)

all: lint test

help:
	@echo ''
	@echo 'Usage: make <OPTIONS> ... <TARGETS>'
	@echo ''
	@echo 'Available Targets are:'
	@echo '  help		Shows this help screen.'
	@echo '  fmt		Reformat with gofmt and goimports'
	@echo '  lint		Run golint, govet, goimports, check format with gofmt'
	@echo '  tools		goget tools needed for commands'
	@echo '  test		Run go test on all packages'
	@echo '  vet		Run go vet on all packages'
	@echo ''
	@echo 'Targets run by default are fmt, imports, vet, lint, test'
	@echo ''

lint: tools
	golint -set_exit_status $(PACKAGES)
	goimports -l -e -d .
	go vet ./...
	$(eval NEEDS_FORMAT := $(shell gofmt -l .))
	@if [ "$(NEEDS_FORMAT)" != "" ] ; then \
		echo "$(NEEDS_FORMAT)"; \
		exit 1; \
	fi;

test:
	go test -race -v ./...

fmt: tools
	go fmt $(PACKAGES)
	go imports -l -w

tools:
	go get -u -v $(GOTOOLS)

.PHONY: help lint test fmt tools
