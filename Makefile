# Makefile for the bitcask-kv-go project
.PHONY: test test-race run-example clean

# 运行所有常规测试
test:
	@echo "==> Running all tests..."
	go test -v ./...

# 运行所有测试并开启竞态检测器
test-race:
	@echo "==> Running all tests with race detector..."
	go test -race -v ./...

# 运行 examples/basic_operation.go 示例
run-example:
	@echo "==> Running basic_operation example..."
	go run ./examples/basic_operation.go

# 清理示例程序生成的数据目录
clean:
	@echo "==> Cleaning up generated data..."
	rm -rf bitcask-data/*
