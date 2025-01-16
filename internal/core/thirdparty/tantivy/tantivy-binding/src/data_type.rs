#[repr(u8)]
#[derive(Debug)]
pub enum TantivyDataType {
    Text,
    Keyword,
    // U64,
    I64,
    F64,
    Bool,
}
