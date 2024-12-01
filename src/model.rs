#[derive(serde::Deserialize, Debug)]
pub struct Series {
    pub tags: Vec<String>,
    #[serde(rename = "tagValues")]
    pub tag_values: Vec<String>,
    pub ts: Vec<i64>,
    #[serde(rename = "sumsDouble")]
    pub sums_double: Vec<f64>,
    #[serde(rename = "sumsLong")]
    pub sums_long: Vec<i64>,
    pub count: Vec<i64>,
}

#[derive(serde::Deserialize, Debug)]
pub struct Metric {
    pub metric: String,
    #[serde(rename = "measurementType")]
    #[allow(unused)]
    pub measurement_type: u8,
    pub series: Vec<Series>,
}
