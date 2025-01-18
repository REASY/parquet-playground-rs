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
    B: ArrayBuilder + NullableBuilder + 'static + std::fmt::Debug,
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
    B: ArrayBuilder + NullableBuilder + 'static + std::fmt::Debug,
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
    let mut total_records_read = 0;

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
            list_builder.append(true);
            let array = list_builder.finish();
            return Ok(Box::new(array));
        }

        let mut val_idx = 0;
        for i in 0..levels_read {
            let dl = def_levels[i];
            let rl = rep_levels[i];

            if rl == 0 && (val_idx > 0 || total_records_read > 0) {
                list_builder.append(true);
            }

            if dl == max_def_level {
                append_fn(&values[val_idx], list_builder.values())?;
                val_idx += 1;
            } else if dl == opt_def_level {
                list_builder.values().append_null_value();
            } else {
                panic!("Unexpected def_level: {dl}");
            }
        }
        total_records_read += records_read;
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
        StringBuilder::new,
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
        Int64Builder::new,
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
        Float64Builder::new,
    )
}

#[cfg(test)]
mod tests {
    use crate::columns_builder::ColumnsBuilder;
    use crate::errors;
    use crate::schema::parquet_metadata_to_arrow_schema;
    use crate::vec_pq_reader::{read_f64_column, read_i64_column, read_string_column};
    use arrow::array::{
        Array, ArrayRef, AsArray, Float64Builder, Int64Builder, ListBuilder, RecordBatch,
        StringBuilder,
    };
    use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Float64Type, Int64Type, Schema};
    use arrow::error::ArrowError;
    use parquet::arrow::arrow_writer::ArrowWriterOptions;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::column::reader::{get_typed_column_reader, ColumnReaderImpl};
    use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::format::FileMetaData;
    use parquet::schema::types::ColumnDescriptor;
    use std::fs::File;
    use std::marker::PhantomData;
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

    pub struct TestBuilder {
        pub schema: Arc<Schema>,
        str_field: StringBuilder,
        list_str_field: ListBuilder<StringBuilder>,
        i64_field: Int64Builder,
        list_i64_field: ListBuilder<Int64Builder>,
        f64_field: Float64Builder,
        list_f64_field: ListBuilder<Float64Builder>,
    }

    #[derive(Clone)]
    pub struct TestBuilderRow {
        str: Option<String>,
        list_str: Vec<Option<String>>,
        i64: Option<i64>,
        list_i64: Vec<Option<i64>>,
        f64: Option<f64>,
        list_f64: Vec<Option<f64>>,
    }

    impl TestBuilderRow {
        fn new(
            str: Option<&str>,
            str_list: Vec<Option<&str>>,
            i64: Option<i64>,
            list_i64: Vec<Option<i64>>,
            f64: Option<f64>,
            list_f64: Vec<Option<f64>>,
        ) -> Self {
            let list_str: Vec<Option<String>> =
                str_list.iter().map(|x| x.map(|c| c.to_owned())).collect();
            TestBuilderRow {
                str: str.map(|x| x.to_owned()),
                list_str,
                i64,
                list_i64,
                f64,
                list_f64,
            }
        }
    }

    impl TestBuilder {
        fn new() -> Self {
            let fields = vec![
                Field::new("str_field", DataType::Utf8, true),
                Field::new(
                    "list_str_field",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    true,
                ),
                Field::new("i64_field", DataType::Int64, true),
                Field::new(
                    "list_i64_field",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
                    true,
                ),
                Field::new("f64_field", DataType::Float64, true),
                Field::new(
                    "list_f64_field",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
                    true,
                ),
            ];
            TestBuilder {
                schema: Arc::new(Schema::new(fields)),
                str_field: StringBuilder::new(),
                list_str_field: ListBuilder::new(StringBuilder::new()),
                i64_field: Int64Builder::new(),
                list_i64_field: ListBuilder::new(Int64Builder::new()),
                f64_field: Float64Builder::new(),
                list_f64_field: ListBuilder::new(Float64Builder::new()),
            }
        }
    }

    impl<'a> ColumnsBuilder<'a> for TestBuilder {
        type T = TestBuilderRow;

        fn get_schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }

        fn get_batch(&mut self) -> Result<RecordBatch, ArrowError> {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(self.str_field.finish()),
                Arc::new(self.list_str_field.finish()),
                Arc::new(self.i64_field.finish()),
                Arc::new(self.list_i64_field.finish()),
                Arc::new(self.f64_field.finish()),
                Arc::new(self.list_f64_field.finish()),
            ];
            RecordBatch::try_new(self.schema.clone(), columns)
        }

        fn append(&mut self, msg: &'a Self::T) -> Result<(), ArrowError> {
            self.str_field.append_option(msg.str.clone());
            self.list_str_field.append_value(msg.list_str.clone());
            self.i64_field.append_option(msg.i64);
            self.list_i64_field.append_value(msg.list_i64.clone());
            self.f64_field.append_option(msg.f64);
            self.list_f64_field.append_value(msg.list_f64.clone());
            Ok(())
        }

        fn reset(&mut self) -> Result<(), ArrowError> {
            self.str_field = StringBuilder::new();
            self.list_str_field = ListBuilder::new(StringBuilder::new());
            self.i64_field = Int64Builder::new();
            self.list_i64_field = ListBuilder::new(Int64Builder::new());
            self.f64_field = Float64Builder::new();
            self.list_f64_field = ListBuilder::new(Float64Builder::new());
            Ok(())
        }
    }

    fn get_rows() -> Vec<TestBuilderRow> {
        vec![
            TestBuilderRow::new(
                None,
                vec![None, Some("1"), None, Some("2")],
                Some(0),
                vec![
                    Some(1),
                    Some(0),
                    Some(2),
                    Some(-1),
                    Some(-2),
                    Some(i64::MAX),
                    Some(i64::MIN),
                ],
                None,
                vec![
                    Some(0.0),
                    Some(1.0),
                    Some(-1.0),
                    Some(f64::MAX),
                    Some(f64::MIN),
                    None,
                ],
            ),
            TestBuilderRow::new(
                None,
                vec![Some("3"), Some("4"), None, Some("5")],
                Some(i64::MAX),
                vec![Some(4)],
                Some(f64::MAX),
                vec![None, None, None, None],
            ),
            TestBuilderRow::new(
                Some("hello"),
                vec![None, None],
                Some(i64::MIN),
                vec![None, None, None, None, None],
                Some(f64::MIN),
                vec![
                    Some(-123.456),
                    Some(-456.789),
                    Some(123.456),
                    Some(456.789),
                    Some(0.0),
                ],
            ),
            TestBuilderRow::new(
                Some("world"),
                vec![Some("6"), Some("7")],
                Some(6),
                vec![Some(7), Some(8), Some(8), Some(8), Some(8)],
                Some(f64::MIN),
                vec![
                    Some(-5.0),
                    Some(5.0),
                    Some(5.5),
                    None,
                    None,
                    Some(6.0123456),
                ],
            ),
            TestBuilderRow::new(
                None,
                vec![None],
                None,
                vec![Some(10), Some(11), Some(12), Some(13), Some(14)],
                Some(1.0),
                vec![None, None, None, Some(100.0), Some(200.0), Some(300.0)],
            ),
            TestBuilderRow::new(
                None,
                vec![None],
                None,
                vec![None, Some(15)],
                Some(0.0),
                vec![None, None, None],
            ),
            TestBuilderRow::new(
                Some("from"),
                vec![Some("8"), Some("9"), None, None, Some("10")],
                None,
                vec![Some(16), Some(17)],
                Some(123456789.01),
                vec![Some(0.0), Some(1.0), Some(2.0)],
            ),
            TestBuilderRow::new(
                Some("Rust"),
                vec![None, None, None, None, None, Some("12")],
                Some(18),
                vec![None, None, Some(19), Some(20)],
                Some(f64::EPSILON),
                vec![Some(f64::EPSILON), Some(f64::EPSILON), Some(f64::MIN)],
            ),
        ]
    }

    trait ScalarReader<T> {
        fn to_vec_option(&self, array: &dyn Array) -> Vec<Option<T>>;
    }

    pub struct PrimitiveReader<A: ArrowPrimitiveType>(PhantomData<A>);
    impl<A: ArrowPrimitiveType> ScalarReader<A::Native> for PrimitiveReader<A> {
        fn to_vec_option(&self, array: &dyn Array) -> Vec<Option<A::Native>> {
            array.as_primitive::<A>().iter().collect()
        }
    }

    pub struct StringReader;
    impl ScalarReader<String> for StringReader {
        fn to_vec_option(&self, array: &dyn Array) -> Vec<Option<String>> {
            array
                .as_string::<i32>()
                .iter()
                .map(|s| s.map(|x| x.to_string()))
                .collect()
        }
    }

    trait ListReader<T> {
        fn to_list_vec(&self, array: &dyn Array) -> Vec<Vec<Option<T>>>;
    }

    pub struct PrimitiveListReader<A: ArrowPrimitiveType>(pub PhantomData<A>);
    impl<A: ArrowPrimitiveType> ListReader<A::Native> for PrimitiveListReader<A> {
        fn to_list_vec(&self, array: &dyn Array) -> Vec<Vec<Option<A::Native>>> {
            let list_array = array.as_list::<i32>();
            (0..list_array.len())
                .map(|idx| {
                    // Downcast the sub-array to the actual primitive array
                    let sub_array = list_array.value(idx);
                    let typed_sub_array = sub_array.as_primitive::<A>();
                    // Collect each element as Option<A::Native>
                    typed_sub_array.iter().collect()
                })
                .collect()
        }
    }

    pub struct StringListReader;
    impl ListReader<String> for StringListReader {
        fn to_list_vec(&self, array: &dyn Array) -> Vec<Vec<Option<String>>> {
            let list_array = array.as_list::<i32>();
            (0..list_array.len())
                .map(|idx| {
                    let sub_array = list_array.value(idx);
                    let typed_sub_array = sub_array.as_string::<i32>();
                    typed_sub_array
                        .iter()
                        .map(|opt_str| opt_str.map(|s| s.to_owned()))
                        .collect()
                })
                .collect()
        }
    }

    struct InitializedParquet {
        #[allow(unused)]
        temp_file: NamedTempFile,
        #[allow(unused)]
        file_metadata: FileMetaData,
        reader: SerializedFileReader<File>,
        schema: Schema,
    }

    fn init_parquet(values: &Vec<TestBuilderRow>) -> errors::Result<InitializedParquet> {
        let mut builder = TestBuilder::new();
        for v in values {
            builder.append(v)?;
        }

        let temp_file: NamedTempFile = NamedTempFile::with_suffix(".parquet")?;
        let path = temp_file.path();
        let md = create_parquet(temp_file.as_file(), Box::new(builder))?;

        let file = File::open(path)?;
        let reader: SerializedFileReader<File> = SerializedFileReader::new(file)?;
        let schema: Schema = parquet_metadata_to_arrow_schema(reader.metadata());
        Ok(InitializedParquet {
            temp_file,
            file_metadata: md,
            reader,
            schema,
        })
    }

    fn test_read_any_scalar_column<ParquetT, T, E, R, S>(
        values: &[TestBuilderRow],
        field_name: &str,
        read_fn: R,
        extract_expected: E,
        scalar_rdr: &S,
    ) -> errors::Result<()>
    where
        ParquetT: parquet::data_type::DataType,
        R: Fn(
            ColumnReaderImpl<ParquetT>,
            &ColumnDescriptor,
            usize,
        ) -> errors::Result<Box<dyn Array>>,
        E: Fn(&TestBuilderRow) -> Option<T>,
        T: PartialEq + std::fmt::Debug,
        S: ScalarReader<T>,
    {
        let inited = init_parquet(&values.to_vec())?;
        let rg = inited.reader.get_row_group(0)?;

        let (field_index, _) = inited
            .schema
            .column_with_name(field_name)
            .ok_or_else(|| ArrowError::InvalidArgumentError(field_name.to_string()))?;
        let field_desc: &ColumnDescriptor = rg.metadata().column(field_index).column_descr();

        // Test with different batch sizes
        for batch_size in 1..=values.len() {
            let col_reader = rg.get_column_reader(field_index)?;
            let typed_reader = get_typed_column_reader::<ParquetT>(col_reader);
            let array = read_fn(typed_reader, field_desc, batch_size)?;

            let read_values = scalar_rdr.to_vec_option(&*array);

            let expected_values: Vec<Option<T>> =
                values.iter().map(|v| extract_expected(v)).collect();

            assert_eq!(expected_values, read_values);
        }
        Ok(())
    }

    fn test_read_any_list_column<ParquetT, T, E, R, S>(
        values: &[TestBuilderRow],
        field_name: &str,
        read_fn: R,
        extract_expected: E,
        list_rdr: &S,
    ) -> errors::Result<()>
    where
        ParquetT: parquet::data_type::DataType,
        R: Fn(
            ColumnReaderImpl<ParquetT>,
            &ColumnDescriptor,
            usize,
        ) -> errors::Result<Box<dyn Array>>,
        E: Fn(&TestBuilderRow) -> &Vec<Option<T>>,
        T: PartialEq + std::fmt::Debug + Clone + std::fmt::Display,
        S: ListReader<T>,
    {
        let inited = init_parquet(&values.to_vec())?;
        let rg = inited.reader.get_row_group(0)?;

        let (field_index, _) = inited
            .schema
            .column_with_name(field_name)
            .ok_or_else(|| ArrowError::InvalidArgumentError(field_name.to_string()))?;
        let field_desc: &ColumnDescriptor = rg.metadata().column(field_index).column_descr();

        // Test with different batch sizes
        for batch_size in 1..=values.len() {
            let col_reader = rg.get_column_reader(field_index)?;
            let typed_reader = get_typed_column_reader::<ParquetT>(col_reader);
            let array = read_fn(typed_reader, field_desc, batch_size)?;
            assert!(array.len() > 0);

            // The trait does the conversion to Vec<Vec<Option<RustVal>>>
            let read_values: Vec<Vec<Option<T>>> = list_rdr.to_list_vec(&*array);
            assert!(read_values.len() > 0);

            // Compare with the "expected" data from `TestBuilderRow`
            let expected_values: Vec<Vec<Option<T>>> = values
                .iter()
                .map(|row| extract_expected(row).clone())
                .collect();
            assert_eq!(expected_values, read_values);
        }
        Ok(())
    }

    #[test]
    fn test_read_for_field_optional_string() -> errors::Result<()> {
        let values = get_rows();
        test_read_any_scalar_column::<parquet::data_type::ByteArrayType, String, _, _, _>(
            &values,
            "str_field",
            read_string_column,
            |r| r.str.clone(),
            &StringReader,
        )?;
        Ok(())
    }

    #[test]
    fn test_read_for_field_list_of_optional_string_field() -> errors::Result<()> {
        let values = get_rows();
        test_read_any_list_column::<parquet::data_type::ByteArrayType, String, _, _, _>(
            &values,
            "list_str_field",
            read_string_column,
            |r| &r.list_str,
            &StringListReader, // the trait implementation for list<utf8>
        )?;
        Ok(())
    }

    #[test]
    fn test_read_for_field_optional_i64() -> errors::Result<()> {
        let values = get_rows();
        test_read_any_scalar_column::<parquet::data_type::Int64Type, i64, _, _, _>(
            &values,
            "i64_field",
            read_i64_column,
            |r| r.i64,
            &PrimitiveReader::<Int64Type>(PhantomData),
        )?;
        Ok(())
    }

    #[test]
    fn test_read_for_field_list_of_optional_i64_field() -> errors::Result<()> {
        let values = get_rows();
        test_read_any_list_column::<parquet::data_type::Int64Type, i64, _, _, _>(
            &values,
            "list_i64_field",
            read_i64_column,
            |r| &r.list_i64,
            &PrimitiveListReader::<Int64Type>(PhantomData),
        )?;
        Ok(())
    }

    #[test]
    fn test_read_for_field_optional_f64() -> errors::Result<()> {
        let values = get_rows();
        test_read_any_scalar_column::<parquet::data_type::DoubleType, f64, _, _, _>(
            &values,
            "f64_field",
            read_f64_column,
            |r| r.f64,
            &PrimitiveReader::<Float64Type>(PhantomData),
        )?;
        Ok(())
    }

    #[test]
    fn test_read_for_field_list_of_optional_f64_field() -> errors::Result<()> {
        let values = get_rows();
        test_read_any_list_column::<parquet::data_type::DoubleType, f64, _, _, _>(
            &values,
            "list_f64_field",
            read_f64_column,
            |r| &r.list_f64,
            &PrimitiveListReader::<Float64Type>(PhantomData),
        )?;
        Ok(())
    }
}
