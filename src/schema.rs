use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use parquet::basic::{ConvertedType, Repetition};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::{Type as ParquetType, TypePtr};
use std::sync::Arc;

#[derive(Debug)]
pub struct ThisSchema {
    pub schema: Schema,
}

impl ThisSchema {
    fn f64_list() -> DataType {
        DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true)))
    }
    pub fn i64_list() -> DataType {
        DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)))
    }

    pub fn ts_field() -> Field {
        Field::new("ts", ThisSchema::i64_list(), true)
    }

    pub fn sums_double_field() -> Field {
        Field::new("sums_double", ThisSchema::f64_list(), true)
    }

    pub fn sums_long_field() -> Field {
        Field::new("sums_long", ThisSchema::i64_list(), true)
    }

    pub fn count_field() -> Field {
        Field::new("count", ThisSchema::i64_list(), true)
    }

    pub fn binary_field() -> Field {
        Field::new("binary_data", DataType::Binary, true)
    }

    pub fn new(all_tags: &[String], use_flatbuffers: bool) -> ThisSchema {
        let mut all_fields: Vec<Field> = all_tags
            .iter()
            .map(|t| Field::new(t.to_owned(), DataType::Utf8, true))
            .collect();
        if !use_flatbuffers {
            all_fields.push(Self::ts_field());
            all_fields.push(Self::sums_double_field());
            all_fields.push(Self::sums_long_field());
            all_fields.push(Self::count_field());
        } else {
            all_fields.push(Self::binary_field());
        }
        let schema = Schema::new(all_fields);
        ThisSchema { schema }
    }
}

pub fn parquet_metadata_to_arrow_schema(metadata: &ParquetMetaData) -> Schema {
    let parquet_schema = metadata.file_metadata().schema();
    let fields = convert_parquet_schema_to_arrow_fields(parquet_schema.get_fields());
    Schema::new(fields)
}

fn convert_parquet_schema_to_arrow_fields(fields: &[TypePtr]) -> Vec<Field> {
    fields
        .iter()
        .map(|field| convert_parquet_field_to_arrow_field(field))
        .collect()
}

fn convert_parquet_field_to_arrow_field(field: &ParquetType) -> Field {
    let name = field.get_basic_info().name().to_string();
    let nullable = field.get_basic_info().repetition() == Repetition::OPTIONAL;
    let data_type = convert_parquet_type_to_arrow_type(field);
    Field::new(name, data_type, nullable)
}

fn convert_parquet_type_to_arrow_type(field: &ParquetType) -> DataType {
    match field {
        ParquetType::PrimitiveType {
            physical_type,
            type_length,
            ..
        } => match physical_type {
            parquet::basic::Type::BOOLEAN => DataType::Boolean,
            parquet::basic::Type::INT32 => DataType::Int32,
            parquet::basic::Type::INT64 => DataType::Int64,
            parquet::basic::Type::INT96 => DataType::Timestamp(TimeUnit::Nanosecond, None),
            parquet::basic::Type::FLOAT => DataType::Float32,
            parquet::basic::Type::DOUBLE => DataType::Float64,
            parquet::basic::Type::BYTE_ARRAY => {
                if field.get_basic_info().converted_type() == ConvertedType::UTF8 {
                    DataType::Utf8
                } else {
                    DataType::Binary
                }
            }
            parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => DataType::FixedSizeBinary(*type_length),
        },
        ParquetType::GroupType { fields, .. } => {
            if is_list_type(field) {
                let element_type = &fields[0].get_fields()[0];
                DataType::List(Arc::new(convert_parquet_field_to_arrow_field(element_type)))
            } else {
                DataType::Struct(Fields::from(convert_parquet_schema_to_arrow_fields(fields)))
            }
        }
    }
}

fn is_list_type(field: &ParquetType) -> bool {
    if let ParquetType::GroupType { fields, .. } = field {
        if fields.len() == 1 {
            if let ParquetType::GroupType {
                basic_info,
                fields: inner_fields,
                ..
            } = fields[0].as_ref()
            {
                return basic_info.name() == "list" && inner_fields.len() == 1;
            }
        }
    }
    false
}
