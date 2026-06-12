UNAME    := $(shell uname -s)
DIST     := dist/linux
SERVICES := aggregator spread-calculator execution-engine telegram-notifier fill-tracker position-manager

ifeq ($(UNAME),Linux)
    BUILD_CMD := cargo build --release
    BIN_DIR   := target/release
else
    BUILD_CMD := cargo zigbuild --release --target x86_64-unknown-linux-gnu.2.17
    BIN_DIR   := target/x86_64-unknown-linux-gnu/release
endif

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
