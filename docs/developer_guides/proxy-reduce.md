# How Proxy Reduces the Multiple Search Results?

## Search Results Organized format

For a specified Search request, with nq = 2 and topk = 10, Proxy receives 4 search results from query nodes.

Each Search Result is nq * topk two-dimensional structure, as the illustration below. The result indicates that the user input nq vectors and wanted to obtain the topk similar vectors to these nq vectors respectively.

![search_result_format](./figs/nq_topk_search_results.png)

For each query, the topk hit results are in descending order of score. The larger the score, the more similar the hit result is to the vector to be queried. The hit results of different queries are independent of each other.

Therefore, we will only discuss how Proxy merges the results for one query result. For nq query results, we can loop through nq or process them in parallel.

So the problem degenerates to how to get the maximum number of 10 (topk) results from these four sorted arrays. As shown in the figure below:
![final_result](./figs/reduce_results.png)

## K-Way Merge Algorithm

The pseudocode of this algorithm is shown below:

```golang
n = 4
multiple_results = [[topk results 1], [topk results 2], [topk results 3], [topk results 4]]
locs = [0, 0, 0, 0]
topk_results = []
for i -> topk:
	score = min_score
	choice = -1
	for j -> n:
		choiceOffset = locs[j]
		if choiceOffset > topk:
			// all result from this way has been got, got from other way
			continue
		score_this_way = multiple_results[j][choiceOffset]
		if score_this_way > score:
			choice = j
			score = score_this_way
	if choice != -1:
		// update location
		locs[choice]++
		topk_results = append(topk_results, choice)
```

This algorithm is originated from the merging phase of merge sort. The common point of the two is that the results have been sorted when merging, and the difference is that merge sort merges two-way results, proxy reduces merges multiple results.

In contrast, in merge sort, two pointers are used to record the offsets of the two-way results, and proxy reduces uses multiple pointers `locs` to record the offsets of the `k-way` results.

In our specific situation, n indicates that there are 4 results to be merged, `multiple_results` is an array of four `topk`, and each `choiceOffset` in `locs` records the offset of each way being merged.

The `score_this_way` corresponding to this offset records the maximum value of the current way, so when you take a larger `score`, you only need to pick one of the four maximum values.

This ensures that the result we take each time is the largest among the remaining results.

This algorithm will scan all Search Results linearly at most, hence the time complexity of this algorithm is n \* topk.
