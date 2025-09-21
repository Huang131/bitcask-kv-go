    # Makefile for the bitcask-kv-go project
    .PHONY: test test-race

    # 运行所有常规测试
    test:
		@echo "==> Running all tests..."
		go test -v ./...

    # 运行所有测试并开启竞态检测器
    test-race:
		@echo "==> Running all tests with race detector..."
		go test -race -v ./...
