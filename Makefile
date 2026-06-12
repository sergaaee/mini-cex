UNAME    := $(shell uname -s)
TARGET   := x86_64-unknown-linux-gnu.2.17
DIST     := dist/linux
SERVICES := aggregator spread-calculator execution-engine telegram-notifier fill-tracker position-manager

BUILD_CMD := cargo zigbuild --release --target $(TARGET)
BIN_DIR   := target/x86_64-unknown-linux-gnu/release

.PHONY: build dist docker-build up clean

build:
	$(BUILD_CMD)

dist: build
	mkdir -p $(DIST)
	$(foreach svc,$(SERVICES),cp $(BIN_DIR)/$(svc) $(DIST)/$(svc);)

docker-build: dist
	sudo docker compose build

up: dist
	sudo docker compose up -d

clean:
	cargo clean
	rm -rf $(DIST)
