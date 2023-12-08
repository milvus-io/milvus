#!/bin/bash

for i in $(seq 0 150)
do
pytest --host 10.102.6.222 testcases/test_search.py::TestCollectionSearch::test_search_output_field_vector_after_different_index_metrics
pytest --host 10.102.6.222 testcases/test_search.py::TestCollectionSearch::test_search_output_field_vector_after_different_index_metrics
done