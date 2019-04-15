struct test1 {
  1: i32 one
  2: i32 two                    // omit
  3: i32 three
}

struct test2 {
  1: i32 one
  2: test2 two                  // omit
  3: i32 three
}

struct test3 {
  1: i32 one
  2: list<test1> two            // omit
}

struct test4 {
  1: i32 one
  2: map<i32,test1> two         // omit
}

