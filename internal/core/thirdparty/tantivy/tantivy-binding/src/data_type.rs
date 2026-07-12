#[allow(dead_code)]
#[repr(u8)]
#[derive(Debug)]
pub enum TantivyDataType {
    Text,
    Keyword,
    // U64,
    I64,
    F64,
    Bool,
    JSON,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonExistValueType {
    Any,
    Numeric,
    String,
    Bool,
}
