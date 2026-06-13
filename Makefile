.PHONY: all build clean run generate-config

APP_NAME=pg-wal-stream

all: build generate-config

build:
	@echo "Building $(APP_NAME)..."
	go build -o $(APP_NAME) main.go
	@echo "Build successful!"

generate-config: build
	@echo "Generating example configuration file (config.example.yaml)..."
	./$(APP_NAME) -init

clean:
	@echo "Cleaning up..."
	rm -f $(APP_NAME)
	rm -f config.example.yaml

run: build
	@echo "Running $(APP_NAME)..."
	./$(APP_NAME) -config config.yaml
