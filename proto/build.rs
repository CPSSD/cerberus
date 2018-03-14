extern crate grpc_compiler;
extern crate protobuf;

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use grpc_compiler::codegen as grpc_gen;
use protobuf::codegen as pb_gen;
use protobuf::compiler_plugin::GenResult;
use protobuf::descriptor::FileDescriptorSet;

fn write_output_files(output_path: PathBuf, results: Vec<GenResult>) {
    for res in results {
        let out_file_path: PathBuf = output_path.join(&res.name);
        let mut out_file = File::create(&out_file_path).unwrap();
        out_file.write_all(&res.content).unwrap();
    }
}

fn compile(proto_name: &str) {
    let output_path = Path::new("src");
    let proto_desc_path = output_path.join(&format!("{}.desc", proto_name));

    let mut protoc = Command::new("protoc");
    protoc.args(&["-o", &format!("{}", proto_desc_path.display())]);
    protoc.arg(format!("{}.proto", proto_name));
    let status = protoc.status().unwrap();
    if !status.success() {
        panic!("failed to run {:?}: {}", protoc, status);
    }

    let mut proto_desc_file = File::open(proto_desc_path).unwrap();
    let proto_desc: FileDescriptorSet = protobuf::parse_from_reader(&mut proto_desc_file).unwrap();

    let proto_results: Vec<GenResult> = pb_gen::gen(
        proto_desc.get_file(),
        &[format!("{}.proto", proto_name).to_owned()],
    );
    write_output_files(output_path.to_path_buf(), proto_results);

    let grpc_results: Vec<GenResult> = grpc_gen::gen(
        proto_desc.get_file(),
        &[format!("{}.proto", proto_name).to_owned()],
    );
    write_output_files(output_path.to_path_buf(), grpc_results);
}

fn main() {
    compile("mapreduce");
    compile("worker");
    compile("filesystem");
}
