#[derive(serde::Deserialize, Debug)]
pub struct Series {
    pub tags: Vec<String>,
    #[serde(rename = "tagValues")]
    pub tag_values: Vec<String>,
    pub ts: Vec<i64>,
    #[serde(rename = "sumsDouble")]
    pub sums_double: Vec<Option<f64>>,
    #[serde(rename = "sumsLong")]
    pub sums_long: Vec<Option<i64>>,
    pub count: Vec<i64>,
}

#[derive(serde::Deserialize, Debug)]
pub struct Metric {
    pub metric: String,
    pub series: Vec<Series>,
}
