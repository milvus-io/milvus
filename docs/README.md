# Docs

This repository contains test reports on the search performance of different index types on standalone Milvus.

The tests are run on [SIFT1B dataset](http://corpus-texmex.irisa.fr/), and provide results on the following measures:

- Query Elapsed Time: Time cost (in seconds) to run a query. 

- Recall: The fraction of the total amount of relevant instances that were actually retrieved.

Test variables are `nq` and `topk`.
