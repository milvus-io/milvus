# Docs

This repository contains test reports on the search performance of different index types on standalone Milvus.

The tests are run on [SIFT1B dataset](http://corpus-texmex.irisa.fr/), and provide results on the following measures:

- Query Elapsed Time: Time cost (in seconds) to run a query. 

- Recall: The fraction of the total amount of relevant instances that were actually retrieved.

Test variables are `nq` and `topk`.

## Test reports

The following is a list of existing test reports:

- [IVF_SQ8](test_report/milvus_ivfsq8_test_report_detailed_version.md)
- [IVF_SQ8H](test_report/milvus_ivfsq8h_test_report_detailed_version.md)
- [IVFLAT](test-report/ivfflat_test_report_en.md)

To read the CN version of these reports:

- [IVF_SQ8_cn](test_report/milvus_ivfsq8_test_report_detailed_version_cn.md)
- [IVF_SQ8H_cn](test_report/milvus_ivfsq8h_test_report_detailed_version_cn.md)
- [IVFLAT_cn](test-report/ivfflat_test_report_cn.md)
