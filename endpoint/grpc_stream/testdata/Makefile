PROTO_FILE = ./api/ble/v1/ble.proto
GO_OUT_DIR = .
BINARY_NAME = ble_server
MAIN_FILE = main.go

.PHONY: all proto build clean

all: proto build

proto:
	@echo "Generating protobuf code..."
	protoc --go_out=$(GO_OUT_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT_DIR) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_FILE)
	@echo "Proto generation completed"

build:
	@echo "Building binary..."
	go build -o $(BINARY_NAME) $(MAIN_FILE)
	@echo "Build completed"

# 直接运行
run: proto build
	@echo "Starting server..."
	./$(BINARY_NAME)

clean:
	@echo "Cleaning..."
	rm -f $(BINARY_NAME)
	@echo "Clean completed"