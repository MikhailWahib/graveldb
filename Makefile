.PHONY: compile-tests compile-specific-test debug-test clean

BIN_DIR = ./bin/tests

$(shell mkdir -p $(BIN_DIR))

test-all:
	go test -race ./...

test-all-v:
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


clean:
	@echo "Cleaning up compiled test binaries..."
	@rm -rf $(BIN_DIR)/*
