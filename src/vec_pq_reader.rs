use crate::errors;
use arrow::array::{Array, ArrayBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder};
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{ByteArrayType, DataType, DoubleType, Int64Type};
use parquet::schema::types::ColumnDescriptor;

pub trait NullableBuilder: ArrayBuilder {
    fn append_null_value(&mut self);
}

impl NullableBuilder for Int64Builder {
    fn append_null_value(&mut self) {
        self.append_null()
    }
}

impl NullableBuilder for Float64Builder {
    fn append_null_value(&mut self) {
        self.append_null()
    }
}

impl NullableBuilder for StringBuilder {
    fn append_null_value(&mut self) {
        self.append_null()
    }
}

impl<B: ArrayBuilder + NullableBuilder> NullableBuilder for ListBuilder<B> {
    fn append_null_value(&mut self) {
        self.append(false)
    }
}

fn read_column<T, B, A, F>(
    typed_rdr: ColumnReaderImpl<T>,
    col_desc: &ColumnDescriptor,
    batch_size: usize,
    append_fn: A,
    factory_fn: F,
) -> errors::Result<Box<dyn Array>>
where
    T: DataType,
    B: ArrayBuilder + NullableBuilder + 'static,
    A: Fn(&T::T, &mut B) -> errors::Result<()>,
    F: Fn() -> B,
{
    if col_desc.max_rep_level() == 0 {
        read_simple_column::<T, B, A, F>(typed_rdr, batch_size, append_fn, &factory_fn)
    } else {
        read_repeated_column::<T, B, A, F>(typed_rdr, col_desc, batch_size, append_fn, &factory_fn)
    }
}

fn read_simple_column<T, B, A, F>(
    mut typed_rdr: ColumnReaderImpl<T>,
    batch_size: usize,
    append_fn: A,
    factory_fn: &F,
) -> errors::Result<Box<dyn Array>>
where
    T: DataType,
    B: ArrayBuilder + NullableBuilder + 'static,
    A: Fn(&T::T, &mut B) -> errors::Result<()>,
    F: Fn() -> B,
{
    let mut def_levels = Vec::new();
    let mut values = Vec::new();
    let mut builder = factory_fn();

    loop {
        def_levels.clear();
        values.clear();

        let (records_read, values_read, levels_read) =
            typed_rdr.read_records(batch_size, Some(&mut def_levels), None, &mut values)?;

        if records_read == 0 && values_read == 0 && levels_read == 0 {
            return Ok(Box::new(builder.finish()));
        }

        let mut val_idx = 0;
        for &dl in &def_levels[..levels_read] {
            if dl == 0 {
                builder.append_null_value();
            } else {
                append_fn(&values[val_idx], &mut builder)?;
                val_idx += 1;
            }
        }
    }
}

fn read_repeated_column<T, B, A, F>(
    mut typed_rdr: ColumnReaderImpl<T>,
    col_desc: &ColumnDescriptor,
    batch_size: usize,
    append_fn: A,
    factory_fn: &F,
) -> errors::Result<Box<dyn Array>>
where
    T: DataType,
    B: ArrayBuilder + NullableBuilder + 'static,
    A: Fn(&T::T, &mut B) -> errors::Result<()>,
    F: Fn() -> B,
{
    let max_def_level = col_desc.max_def_level();
    let opt_def_level = max_def_level.saturating_sub(1);

    let inner_builder = factory_fn();
    let mut list_builder = ListBuilder::new(inner_builder);

    let mut def_levels = Vec::new();
    let mut rep_levels = Vec::new();
    let mut values = Vec::new();
    let mut rows = 0;

    loop {
        def_levels.clear();
        rep_levels.clear();
        values.clear();

        let (records_read, values_read, levels_read) = typed_rdr.read_records(
            batch_size,
            Some(&mut def_levels),
            Some(&mut rep_levels),
            &mut values,
        )?;

        if records_read == 0 && values_read == 0 && levels_read == 0 {
            return Ok(Box::new(list_builder.finish()));
        }

        let mut val_idx = 0;
        for i in 0..levels_read {
            let dl = def_levels[i];
            let rl = rep_levels[i];

            if rl == 0 && (val_idx > 0 || rows > 0) {
                list_builder.append(true);
                rows += 1;
            }

            if dl == max_def_level {
                append_fn(&values[val_idx], list_builder.values())?;
                val_idx += 1;
            } else if dl == opt_def_level {
                list_builder.values().append_null_value();
            } else {
                // println!("Unexpected def_level: {dl}");
            }
        }
    }
}

pub fn read_string_column(
    typed_rdr: ColumnReaderImpl<ByteArrayType>,
    col_desc: &ColumnDescriptor,
    batch_size: usize,
) -> errors::Result<Box<dyn Array>> {
    read_column::<ByteArrayType, StringBuilder, _, _>(
        typed_rdr,
        col_desc,
        batch_size,
        |val, builder| {
            let s = val.as_utf8()?;
            builder.append_value(s);
            Ok(())
        },
        || StringBuilder::new(),
    )
}

pub fn read_i64_column(
    typed_rdr: ColumnReaderImpl<Int64Type>,
    col_desc: &ColumnDescriptor,
    batch_size: usize,
) -> errors::Result<Box<dyn Array>> {
    read_column::<Int64Type, Int64Builder, _, _>(
        typed_rdr,
        col_desc,
        batch_size,
        |val, builder| {
            builder.append_value(*val);
            Ok(())
        },
        || Int64Builder::new(),
    )
}

pub fn read_f64_column(
    typed_rdr: ColumnReaderImpl<DoubleType>,
    col_desc: &ColumnDescriptor,
    batch_size: usize,
) -> errors::Result<Box<dyn Array>> {
    read_column::<DoubleType, Float64Builder, _, _>(
        typed_rdr,
        col_desc,
        batch_size,
        |val, builder| {
            builder.append_value(*val);
            Ok(())
        },
        || Float64Builder::new(),
    )
}

#[cfg(test)]
mod tests {
    use crate::columns_builder::ColumnsBuilder;
    use crate::errors;
    use crate::schema::parquet_metadata_to_arrow_schema;
    use crate::vec_pq_reader::read_string_column;
    use arrow::array::{ArrayRef, AsArray, ListBuilder, RecordBatch, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::error::ArrowError;
    use parquet::arrow::arrow_writer::ArrowWriterOptions;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::column::reader::get_typed_column_reader;
    use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::format::FileMetaData;
    use parquet::schema::types::ColumnDescriptor;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    fn create_parquet<T>(
        file: &File,
        mut builder: Box<dyn ColumnsBuilder<T = T>>,
    ) -> errors::Result<FileMetaData> {
        let compression = Compression::ZSTD(ZstdLevel::try_new(3)?);
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_compression(compression)
            .set_dictionary_enabled(true)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();
        let wrt_opts = ArrowWriterOptions::new()
            .with_properties(props)
            .with_skip_arrow_metadata(true);
        let mut writer = ArrowWriter::try_new_with_options(file, builder.get_schema(), wrt_opts)?;
        let batch = builder.get_batch()?;
        writer.write(&batch)?;
        let md = writer.close()?;
        Ok(md)
    }

    pub struct StrBuilder {
        pub schema: Arc<Schema>,
        str_field: StringBuilder,
        list_str_field: ListBuilder<StringBuilder>,
    }

    pub struct StrBuilderRow {
        str: Option<String>,
        list_str: Vec<Option<String>>,
    }

    impl StrBuilderRow {
        fn new(str: Option<&str>, list: Vec<Option<&str>>) -> Self {
            let list_str: Vec<Option<String>> =
                list.iter().map(|x| x.map(|c| c.to_owned())).collect();
            StrBuilderRow {
                str: str.map(|x| x.to_owned()),
                list_str,
            }
        }
    }

    impl StrBuilder {
        fn new() -> Self {
            let fields = vec![
                Field::new("str_field", DataType::Utf8, true),
                Field::new(
                    "list_str_field",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    true,
                ),
            ];
            StrBuilder {
                schema: Arc::new(Schema::new(fields)),
                str_field: StringBuilder::new(),
                list_str_field: ListBuilder::new(StringBuilder::new()),
            }
        }
    }

    impl<'a> ColumnsBuilder<'a> for StrBuilder {
        type T = StrBuilderRow;

        fn get_schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }

        fn get_batch(&mut self) -> Result<RecordBatch, ArrowError> {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(self.str_field.finish()),
                Arc::new(self.list_str_field.finish()),
            ];
            RecordBatch::try_new(self.schema.clone(), columns)
        }

        fn append(&mut self, msg: &'a Self::T) -> Result<(), ArrowError> {
            self.str_field.append_option(msg.str.clone());
            self.list_str_field.append_value(msg.list_str.clone());
            Ok(())
        }

        fn reset(&mut self) -> Result<(), ArrowError> {
            self.str_field = StringBuilder::new();
            self.list_str_field = ListBuilder::new(StringBuilder::new());
            Ok(())
        }
    }

    fn get_str_values() -> Vec<StrBuilderRow> {
        vec![
            StrBuilderRow::new(None, vec![None, Some("1"), None, Some("2")]),
            StrBuilderRow::new(None, vec![Some("3"), Some("4"), None, Some("5")]),
            StrBuilderRow::new(Some("hello"), vec![None, None]),
            StrBuilderRow::new(Some("world"), vec![Some("6"), Some("7")]),
            StrBuilderRow::new(None, vec![None]),
            StrBuilderRow::new(None, vec![None]),
            StrBuilderRow::new(
                Some("from"),
                vec![Some("8"), Some("9"), None, None, Some("10")],
            ),
            StrBuilderRow::new(Some("Rust"), vec![None, None, None, None, None, Some("12")]),
        ]
    }

    #[test]
    fn test_read_for_field_optional_string() -> errors::Result<()> {
        let values: Vec<StrBuilderRow> = get_str_values();
        let mut builder = StrBuilder::new();
        for v in &values {
            builder.append(v)?;
        }
        let temp_file = NamedTempFile::with_suffix(".parquet")?;
        let path = temp_file.path();
        let _md = create_parquet(temp_file.as_file(), Box::new(builder))?;

        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let schema = parquet_metadata_to_arrow_schema(reader.metadata());

        let rg = reader.get_row_group(0)?;

        let str_field_col = schema.column_with_name("str_field").unwrap();
        let str_field_col_desc: &ColumnDescriptor =
            rg.metadata().column(str_field_col.0).column_descr();

        for batch_size in 1..=values.len() {
            let col_rdr = get_typed_column_reader::<parquet::data_type::ByteArrayType>(
                rg.get_column_reader(str_field_col.0)?,
            );
            let read_data = read_string_column(col_rdr, str_field_col_desc, batch_size)?;
            let read_values: Vec<Option<String>> = read_data
                .as_string::<i32>()
                .iter()
                .map(|c| c.map(|x| x.to_owned()))
                .collect();
            let expected_values: Vec<Option<String>> =
                values.iter().map(|c| c.str.clone()).collect();
            assert_eq!(expected_values, read_values);
        }
        Ok(())
    }

    #[test]
    fn test_read_for_field_list_of_optional_string_field() -> errors::Result<()> {
        let values: Vec<StrBuilderRow> = get_str_values();
        let mut builder = StrBuilder::new();
        for v in &values {
            builder.append(v)?;
        }

        let temp_file = NamedTempFile::with_suffix(".parquet")?;
        let path = temp_file.path();
        let _md = create_parquet(temp_file.as_file(), Box::new(builder))?;

        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let schema = parquet_metadata_to_arrow_schema(reader.metadata());

        let rg = reader.get_row_group(0)?;

        let str_field_col = schema.column_with_name("list_str_field").unwrap();
        let str_field_col_desc: &ColumnDescriptor =
            rg.metadata().column(str_field_col.0).column_descr();

        for batch_size in 1..=values.len() {
            let col_rdr = get_typed_column_reader::<parquet::data_type::ByteArrayType>(
                rg.get_column_reader(str_field_col.0)?,
            );
            let read_data = read_string_column(col_rdr, str_field_col_desc, batch_size)?;
            let read_list = read_data.as_list::<i32>();
            let mut idx: usize = 0;
            for r in read_list.iter() {
                let arr = r.unwrap();
                let str_arr = arr.as_string::<i32>();
                let read: Vec<Option<String>> =
                    str_arr.iter().map(|c| c.map(|x| x.to_owned())).collect();

                let expected = &values[idx].list_str;
                assert_eq!(*expected, read);
                idx += 1;
            }
        }
        Ok(())
    }
}
