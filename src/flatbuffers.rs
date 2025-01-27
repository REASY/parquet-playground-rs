use crate::model::Series;
use flatbuffers::FlatBufferBuilder;

#[allow(dead_code, unused_imports)]
#[path = "../target/flatbuffers/histogram_generated.rs"]
pub mod histogram_flatbuffers;

use crate::flatbuffers::histogram_flatbuffers::{root_as_histogram, Histogram, HistogramArgs};

pub fn to_flatbuffers(msg: &Series) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::with_capacity(1024);
    let mut sums_double: Vec<f64> = Vec::new();
    let mut sums_long: Vec<i64> = Vec::new();
    for d in &msg.sums_double {
        match d {
            None => {}
            Some(d) => {
                sums_double.push(*d);
            }
        }
    }

    for d in &msg.sums_long {
        match d {
            None => {}
            Some(d) => {
                sums_long.push(*d);
            }
        }
    }

    let hist_args = HistogramArgs {
        ts: Some(builder.create_vector(msg.ts.as_slice())),
        count: Some(builder.create_vector(msg.count.as_slice())),
        sums_double: Some(builder.create_vector(sums_double.as_slice())),
        sums_long: Some(builder.create_vector(sums_long.as_slice())),
    };
    let h = Histogram::create(&mut builder, &hist_args);
    builder.finish(h, None);
    builder.finished_data().to_vec()
}

pub fn from_flatbuffers(buf: &[u8]) -> Histogram {
    root_as_histogram(buf).expect("root histogram failed")
}
