# cerberus
CA4019 Project

---

## Building

### Generating Protobufs

The project uses protobufs which need to be generated for a specific language

1. Install protoc: https://github.com/google/protobuf
2. Run `cd proto && make setup`
3. Run `cd proto && make`
4. If you make any changes to the protos run `cd proto && make clean` then
   repeat step 3.

### Building the Project

Build everything by running

```
$ cargo build --all
```
