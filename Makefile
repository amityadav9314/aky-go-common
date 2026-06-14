.PHONY: build test tidy check sort_imports

build:
	@echo "Building aky-go-common..."
	go build ./...

test:
	@echo "Testing aky-go-common..."
	go test ./...

tidy:
	@echo "Tidying aky-go-common..."
	go mod tidy

check: tidy test build
	@echo "All checks passed in aky-go-common! ✅"

sort_imports:
	@echo "Sorting imports..."
	go run golang.org/x/tools/cmd/goimports@latest -w .
