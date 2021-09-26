# Milvus 2.0 vs. 1.x: Cloud-native, distributed architecture, highly scalable, and more

<table class="comparison">
<thead>
	<tr>
		<th>&nbsp;</th>
		<th>Milvus 1.x</th>
		<th>Milvus 2.0</th>
	</tr>
	<tr>
		<th>Architecture</th>
		<td>Shared storage</td>
		<td>Cloud native</td>
	</tr>
</thead>
<tbody>
	<tr>
		<th>Scalability</th>
		<td>1 to 32 read nodes with only one write node</td>
		<td>500+ nodes</td>
	</tr>
  	<tr>
		<th>Durability</th>
		<td><li>Local disk</li><li>Network file system (NFS)</li></td>
		<td><li>Object storage service (OSS)</li><li>Distributed file system (DFS)</li></td>
	</tr>
  	<tr>
		<th>Availability</th>
		<td>99%</td>
		<td>99.9%</td>
	</tr>
	<tr>
		<th>Data consistency</th>
		<td>Eventual consistency</td>
		<td>Three levels of consistency:<li>Strong</li><li>Bounded Staleness</li><li>Session</li><li>Consistent prefix</li></td>
	</tr>
	<tr>
		<th>Data types supported</th>
		<td>Vectors</td>
		<td><li>Vectors</li><li>Fixed-length scalars</li><li>String and text (in planning)</li></td>
	</tr>
	<tr>
		<th>Basic operations supported</th>
		<td><li>Data insertion</li><li>Data deletion</li><li>Approximate nearest neighbor (ANN) Search</li></td>
		<td><li>Data insertion</li><li>Data deletion (in planning)</li><li>Data query</li><li>Approximate nearest neighbor (ANN) Search</li><li>Recurrent neural network (RNN) search (in planning)</li></td>
	</tr>
	<tr>
		<th>Advanced features</th>
		<td><li>Mishards</li><li>Milvus DM</li></td>
		<td><li>Scalar filtering</li><li>Time Travel</li><li>Multi-site deployment and multi-cloud integration</li><li>Data management tools</li></td>
	</tr>
	<tr>
		<th>Index types and libraries</th>
		<td><li>Faiss</li><li>Annoy</li><li>Hnswlib</li><li>RNSG</li></td>
		<td><li>Faiss</li><li>Annoy</li><li>Hnswlib</li><li>RNSG</li><li>ScaNN (in planning)</li><li>On-disk index (in planning)</li></td>
	</tr>
	<tr>
		<th>SDKs</th>
		<td><li><a href="https://github.com/milvus-io/pymilvus/tree/1.x">Python<a></li><li><a href="https://github.com/milvus-io/milvus-sdk-java">Java</a></li><li><a href="https://github.com/milvus-io/milvus-sdk-go/tree/1.x">Go</a></li><li><a href="https://github.com/milvus-io/milvus/tree/1.1/core/src/server/web_impl">RESTful</a></li><li><a href="https://github.com/milvus-io/milvus/tree/1.1/sdk">C++</a></li></td>
		<td><li><a href="https://github.com/milvus-io/pymilvus">Python<a></li><li><a href="https://github.com/milvus-io/milvus-sdk-node">Node</a></li><li><a href="https://github.com/milvus-io/milvus-sdk-go">Go (under development)</a></li><li><a href="https://github.com/milvus-io/milvus-sdk-java/tree/master">Java (under development)</a></li><li>RESTful (in planning)</li><li>C++ (in planning)</li></td>
	</tr>
	<tr>
		<th>Release status</th>
		<td>Long-term support (LTS)</td>
		<td>Release candidate. A stable version will be released in October.</td>
	</tr>
</tbody>
</table>
