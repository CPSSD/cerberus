all: mapreduce

setup:
	cargo install protobuf
	cargo install grpcio-compiler

clean:
	@rm -rf mapreduce/

mapreduce:
	@mkdir mapreduce
	protoc --rust_out=mapreduce --grpc_out=mapreduce --plugin=protoc-gen-grpc=`which grpc_rust_plugin` mapreduce.proto

.PHONY: mapreduce
