#include <cstdint>
#include "tantivy-binding.h"

int
main(int argc, char* argv[]) {
	auto p = tantivy_create_index("test_field_name", TantivyDataType::I64, "/tmp/inverted-index/test-binding/");

	int64_t arr[] = {1, 2, 3, 4, 5, 6};
	tantivy_index_add_int64s(p, arr, sizeof(arr)/sizeof(int64_t));

	tantivy_finish_index(p);
	tantivy_free_index(p);

	return 0;
}
