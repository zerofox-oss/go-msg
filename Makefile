GOTOOLS = \
	github.com/golang/lint/golint \
	golang.org/x/tools/cmd/goimports

PACKAGES=$(shell go list ./...)
NEEDS_FORMAT=$(shell gofmt -l .)

all: fmt imports vet lint test

check-fmt:
	@if [ "$(NEEDS_FORMAT)" != "" ] ; then \
		echo $(NEEDS_FORMAT); \
		exit 1; \
	fi

fmt:
	go fmt $(PACKAGES)

help:
	@echo ''
	@echo 'Usage: make <OPTIONS> ... <TARGETS>'
	@echo ''
	@echo 'Available Targets are:'
	@echo '  help		Shows this help screen.'
	@echo '  check-fmt	List files whose format differs from gofmt'
	@echo '  fmt		Run gofmt'
	@echo '  lint		Run golint'
	@echo '  imports	Run goimports'
	@echo '  tools		goget tools needed for commands'
	@echo '  test		Run go test on all packages'
	@echo '  vet		Run go vet on all packages'
	@echo ''
	@echo 'Targets run by default are fmt, imports, vet, lint, test'
	@echo ''

lint: tools
	@if [ "`golint ./... | grep -v -e '^vendor' -e '\.pb\.go'`" != "" ] ; then\
		echo `golint ./... | grep -v -e '^vendor' -e '\.pb\.go'`; \
		exit 1; \
	fi \

imports: tools
	goimports -l -w .

test:
	go test ./...

tools:
	go get -u -v $(GOTOOLS)

vet:
	go vet ./...
