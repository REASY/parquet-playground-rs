use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=flatbuffers/histogram.fbs");
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("flatbuffers/histogram.fbs")],
        out_dir: Path::new("target/flatbuffers/"),
        ..Default::default()
    })
    .expect("flatc");
}
