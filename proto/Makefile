all: mapreduce mrworker

setup:
	cargo install protobuf
	cargo install grpcio-compiler

clean:
	@rm -rf mapreduce/
	@rm -rf mrworker/

mapreduce:
	@mkdir mapreduce
	protoc --rust_out=mapreduce --grpc_out=mapreduce --plugin=protoc-gen-grpc=`which grpc_rust_plugin` mapreduce.proto

mrworker:
	@mkdir mrworker 
	protoc --rust_out=mrworker --grpc_out=mrworker --plugin=protoc-gen-grpc=`which grpc_rust_plugin` mrworker.proto

.PHONY: mapreduce mrworker
