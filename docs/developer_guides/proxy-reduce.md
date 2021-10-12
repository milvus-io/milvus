# How Proxy Reduce the Multiple Search Results?

## Search Results Organized format

For a specified Search request, with nq = 2 and topk = 10, Proxy received 4 search results from query nodes.

Each Search Result is nq * topk two dimensional structure，as the illustration below. The result indicates that the user has input NQ vectors and wants to obtain the TOPK most similar vectors to these NQ vectors respectively.。

![search_result_format](./figs/nq_topk_search_results.png)

For each query, the top k hit results are in descending order of score. The larger the score, the more similar the hit result is to the vector to be queried. The hit results of different queries are independent of each other.

Therefore, we will only discuss how the proxy merges the results for one query result. For NQ query results, we can loop through NQ or process them in parallel.

So the problem degenerates to how to get the maximum number of 10 (TOPK) results from these four sorted arrays. As shown in the figure below:
![final_result](./figs/reduce_results.png)