.PHONY: compile-tests compile-specific-test debug-test clean

BIN_DIR = ./bin/tests

$(shell mkdir -p $(BIN_DIR))

test-all:
	go test ./...

test-all-v:
	go test ./... -v

compile-tests:
	@if [ -z "$(TEST)" ]; then \
		echo "Compiling all tests..."; \
		go test -gcflags="all=-N -l" -c ./... -o $(BIN_DIR)/my_test.test; \
	else \
		echo "Compiling specific test $(TEST)..."; \
		go test -gcflags="all=-N -l" -c $(TEST) -o $(BIN_DIR)/$(notdir $(TEST)).test; \
	fi

compile-specific-test:
	@echo "Compiling specific test $(TEST)..."
	@go test -gcflags="all=-N -l" -c $(TEST) -o $(BIN_DIR)/$(notdir $(TEST)).test

debug-test:
	@echo "Running GDB on the test binary..."
	@if [ -z "$(TEST)" ]; then \
		echo "No specific test file provided, compiling all tests."; \
		$(MAKE) compile-tests; \
		gdb $(BIN_DIR)/my_test.test; \
	else \
		$(MAKE) compile-specific-test; \
		gdb $(BIN_DIR)/$(notdir $(TEST)).test; \
	fi

clean:
	@echo "Cleaning up compiled test binaries..."
	@rm -rf $(BIN_DIR)/*