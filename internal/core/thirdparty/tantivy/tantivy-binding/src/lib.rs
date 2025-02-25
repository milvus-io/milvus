mod array;
mod data_type;
mod demo_c;
mod docid_collector;
mod error;
mod hashmap_c;
mod index_reader;
mod index_reader_c;
mod index_reader_text;
mod index_reader_text_c;
mod index_writer;
mod index_writer_c;
mod index_writer_text;
mod index_writer_text_c;
mod log;
mod string_c;
mod token_stream_c;
mod analyzer;
mod tokenizer_c;
mod util;
mod util_c;
mod vec_collector;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
