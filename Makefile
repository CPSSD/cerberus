all: release docker

release:
	cargo build --all --release

docker: release
	docker build -t voytechnology/cerberus-master -f Dockerfile.master .
	docker build -t voytechnology/cerberus-worker -f Dockerfile.worker .

.PHONY: docker release clean-docker

compose: clean-docker docker
	docker-compose -f examples/cloud/cerberus-docker/docker-compose.yml -p cerberus up -d --force-recreate

clean-docker:
	docker rmi voytechnology/cerberus-master -f
	docker rmi voytechnology/cerberus-worker -f

compose-down:
	docker-compose -f examples/cloud/cerberus-docker/docker-compose.yml -p cerberus down

clean: compose-down clean-docker
	cargo clean
