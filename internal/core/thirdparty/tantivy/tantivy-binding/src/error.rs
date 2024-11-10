use serde_json as json;

#[derive(Debug)]
pub struct TantivyError{
    reason: String,
}

impl TantivyError{
    fn new(reason:String) -> Self{
        TantivyError{reason:reason}
    }

    pub fn reason(&self) -> String{
        return self.reason.clone()
    }
}

impl From<&str> for TantivyError{
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

impl From<String> for TantivyError{
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<json::Error> for TantivyError{
    fn from(value: json::Error) -> Self {
        Self::new(value.to_string())
    }
}

impl ToString for TantivyError{
    fn to_string(&self) -> String {
        return self.reason()
    }
}