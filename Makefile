all: release docker

release:
	cargo build --all --release

docker: release
	docker build -t cpssd/cerberus-master -f master/Dockerfile .
	docker build -t cpssd/cerberus-worker -f worker/Dockerfile .

.PHONY: docker release clean-docker

compose: clean-docker docker
	docker-compose -f examples/cloud/cerberus-docker/docker-compose.yml -p cerberus up -d --scale worker=5 --force-recreate

clean-docker:
	docker rmi cpssd/cerberus-master -f
	docker rmi cpssd/cerberus-worker -f

compose-down:
	docker-compose -f examples/cloud/cerberus-docker/docker-compose.yml -p cerberus down

clean: compose-down clean-docker
	cargo clean
