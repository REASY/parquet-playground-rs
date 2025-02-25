use crate::flatbuffers::to_flatbuffers;
use crate::model::Series;
use crate::schema::ThisSchema;
use arrow::array::{
    ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, ListBuilder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Schema};
use arrow::error::ArrowError;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub trait ColumnsBuilder<'a> {
    type T;

    fn get_schema(&self) -> Arc<Schema>;
    fn get_batch(&mut self) -> Result<RecordBatch, ArrowError>;
    fn append(&mut self, msg: &'a Self::T) -> Result<(), ArrowError>;

    #[allow(unused)]
    fn reset(&mut self) -> Result<(), ArrowError>;
}

pub struct Builders {
    hexify_tag_columns: bool,
    use_flatbuffers: bool,
    pub schema: Arc<Schema>,
    tag_fields: HashMap<String, StringBuilder>,
    ts: ListBuilder<Int64Builder>,
    sums_double: ListBuilder<Float64Builder>,
    sums_long: ListBuilder<Int64Builder>,
    count: ListBuilder<Int64Builder>,
    binary_data: BinaryBuilder,
}

impl<'a> ColumnsBuilder<'a> for Builders {
    type T = Series;

    fn get_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn get_batch(&mut self) -> Result<RecordBatch, ArrowError> {
        // The order in result vector must follow the order of fields in the schema from `ThisSchema::new`
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields.len());
        for f in self.schema.fields() {
            if *f.data_type() == DataType::Utf8 {
                let column = f.name().as_str();
                let str_builder = self.tag_fields.get_mut(column).unwrap();
                columns.push(Arc::new(str_builder.finish()));
            }
        }
        if !self.use_flatbuffers {
            columns.push(Arc::new(self.ts.finish()));
            columns.push(Arc::new(self.sums_double.finish()));
            columns.push(Arc::new(self.sums_long.finish()));
            columns.push(Arc::new(self.count.finish()));
        } else {
            columns.push(Arc::new(self.binary_data.finish()));
        }
        RecordBatch::try_new(self.schema.clone(), columns)
    }

    fn append(&mut self, msg: &'a Self::T) -> Result<(), ArrowError> {
        let mut used_buffers: HashSet<String> = HashSet::new();
        let mut i: usize = 0;
        while i < msg.tags.len() {
            let tag = {
                let tag: &str = msg.tags[i].as_ref();
                if self.hexify_tag_columns {
                    Self::as_hex(tag)
                } else {
                    tag.to_string()
                }
            };
            let tag_value: String = msg.tag_values[i].clone();
            let str_builder =
                self.tag_fields
                    .get_mut(tag.as_str())
                    .ok_or(ArrowError::InvalidArgumentError(format!(
                        "Could not find tag {}",
                        tag
                    )))?;
            str_builder.append_value(tag_value);

            used_buffers.insert(tag);
            i += 1;
        }
        let unused = self
            .tag_fields
            .iter_mut()
            .filter(|(tag_hex, _)| !used_buffers.contains(tag_hex.as_str()));
        for (_, builder) in unused {
            builder.append_null();
        }

        if !self.use_flatbuffers {
            Self::append_to_i64(msg.ts.as_slice(), &mut self.ts);
            Self::append_to_f64(msg.sums_double.as_slice(), &mut self.sums_double);
            Self::append_to_i64_opt(msg.sums_long.as_slice(), &mut self.sums_long);
            Self::append_to_i64(msg.count.as_slice(), &mut self.count);
        } else {
            let buf = to_flatbuffers(msg);
            self.binary_data.append_value(buf);
        }

        Ok(())
    }

    fn reset(&mut self) -> Result<(), ArrowError> {
        let hex_tag_fields: HashMap<String, StringBuilder> = self
            .schema
            .fields
            .iter()
            .filter(|x| *x.data_type() == DataType::Utf8)
            .map(|f| (f.name().to_owned(), StringBuilder::new()))
            .collect();
        self.tag_fields = hex_tag_fields;
        self.ts = ListBuilder::new(Int64Builder::new());
        self.sums_double = ListBuilder::new(Float64Builder::new());
        self.sums_long = ListBuilder::new(Int64Builder::new());
        self.count = ListBuilder::new(Int64Builder::new());
        self.binary_data = BinaryBuilder::new();
        Ok(())
    }
}

impl Builders {
    pub fn new(all_tags: &[String], hexify_tag_columns: bool, use_flatbuffers: bool) -> Builders {
        let schema = ThisSchema::new(all_tags, use_flatbuffers).schema;
        let hex_tag_fields: HashMap<String, StringBuilder> = schema
            .fields
            .iter()
            .filter(|x| *x.data_type() == DataType::Utf8)
            .map(|f| (f.name().to_owned(), StringBuilder::new()))
            .collect();
        Builders {
            hexify_tag_columns,
            use_flatbuffers,
            schema: Arc::new(schema),
            tag_fields: hex_tag_fields,
            ts: ListBuilder::new(Int64Builder::new()),
            sums_double: ListBuilder::new(Float64Builder::new()),
            sums_long: ListBuilder::new(Int64Builder::new()),
            count: ListBuilder::new(Int64Builder::new()),
            binary_data: BinaryBuilder::new(),
        }
    }

    fn append_to_i64(values: &[i64], builder: &mut ListBuilder<Int64Builder>) {
        let vals: Vec<Option<i64>> = values.iter().map(|x| Some(*x)).collect();
        builder.append_value(vals);
    }

    fn append_to_i64_opt(values: &[Option<i64>], builder: &mut ListBuilder<Int64Builder>) {
        let vals: Vec<Option<i64>> = values.iter().map(|x| x.to_owned()).collect();
        builder.append_value(vals);
    }

    fn append_to_f64(values: &[Option<f64>], builder: &mut ListBuilder<Float64Builder>) {
        let vals: Vec<Option<f64>> = values.iter().map(|x| x.to_owned()).collect();
        builder.append_value(vals);
    }

    pub fn as_hex(s: &str) -> String {
        use std::fmt::Write;
        let mut r = String::new();
        for b in s.as_bytes() {
            write!(r, "{:02x}", b).unwrap();
        }
        r
    }
}
