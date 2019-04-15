const string foo = "bar"

struct a_struct {
  1: bool im_true,
  2: bool im_false,
  3: i8 a_bite,
  4: i16 integer16,
  5: i32 integer32,
  6: i64 integer64,
  7: double double_precision,
  8: string some_characters,
  9: string zomg_unicode,
  10: bool what_who,
}

service AService {
  i32 a_procedure(1: i32 arg)
}
