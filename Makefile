all: build

.PHONY: build release test build-docker-images clean clean-all

# Bulds the debug version of cerberus
build:
	cargo build --verbose --all

# Create the release version
release:
	cargo build --verbose --all --release

clean:
	cargo clean

#############################################################

# Runs all the tests
test: unit-test integration-test dfs-integration

unit-test:
	cargo test --verbose --all

integration-test:
	./tests/integration.sh
	./tests/distributed_grep_test.sh
	./tests/state_saving.sh

multi-machine:
	./tests/multi_machine.sh

dfs-integration:
	./tests/dfs_integration.sh

#############################################################

build-docker-images: release
	docker build -t cpssd/cerberus-master -f master/Dockerfile .
	docker build -t cpssd/cerberus-worker -f worker/Dockerfile .

clean-docker-images:
	docker rmi cpssd/cerberus-master -f
	docker rmi cpssd/cerberus-worker -f

clean-docker: clean-docker-images

clean-all: clean-docker clean
