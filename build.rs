fn main() {
    protobuf_codegen::Codegen::new()
        .pure()
        .includes(["src/protos"])
        .input("src/protos/pulsar.proto")
        .cargo_out_dir("protos")
        .run_from_script();
}
