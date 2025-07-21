.PHONY: compile-tests debug-test bench clean

BIN_DIR = ./bin/tests

$(shell mkdir -p $(BIN_DIR))

test:
	go test -race ./... -v

compile-tests:
	@if [ -z "$(TEST)" ]; then \
		echo "Compiling all tests..."; \
		go test -gcflags="all=-N -l" -c ./... -o $(BIN_DIR)/; \
	else \
		echo "Compiling specific test $(TEST)..."; \
		go test -gcflags="all=-N -l" -c $(TEST) -o $(BIN_DIR)/$(notdir $(TEST)).test; \
	fi

debug-test:
	@echo "Running Delve on the test binary..."
	$(MAKE) compile-tests; \
	dlv exec $(BIN_DIR)/$(notdir $(TEST)).test; \


bench:
	go test -bench=. ./internal/bench -benchmem

clean:
	@echo "Cleaning up compiled test binaries..."
	@rm -rf $(BIN_DIR)/*
