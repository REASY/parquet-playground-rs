use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

#[derive(Debug)]
pub struct ThisSchema {
    pub schema: Schema,
}

impl ThisSchema {
    fn f64_list() -> DataType {
        DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true)))
    }
    fn i64_list() -> DataType {
        DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)))
    }

    fn ts_field() -> Field {
        Field::new("ts", ThisSchema::i64_list(), true)
    }

    fn sums_double_field() -> Field {
        Field::new("sums_double", ThisSchema::f64_list(), true)
    }

    fn sums_long_field() -> Field {
        Field::new("sums_long", ThisSchema::i64_list(), true)
    }

    fn count_field() -> Field {
        Field::new("count", ThisSchema::i64_list(), true)
    }

    pub fn new(all_tags: &[String]) -> ThisSchema {
        let mut all_fields: Vec<Field> = all_tags
            .iter()
            .map(|t| Field::new(t.to_owned(), DataType::Utf8, true))
            .collect();
        all_fields.push(Self::ts_field());
        all_fields.push(Self::sums_double_field());
        all_fields.push(Self::sums_long_field());
        all_fields.push(Self::count_field());
        let schema = Schema::new(all_fields);
        ThisSchema { schema }
    }
}
