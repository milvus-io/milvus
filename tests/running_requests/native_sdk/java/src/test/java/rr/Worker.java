package rr;

import com.google.gson.*;
import io.milvus.v2.client.*;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.utility.request.*;
import io.milvus.v2.service.utility.response.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.response.*;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIteratorV2;
import io.milvus.response.QueryResultsWrapper;
import io.grpc.*;
import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;

/** Native SDK calls only; Python supplies common data, scheduling and assertions. */
public class Worker {
 static final Gson G = new GsonBuilder().serializeNulls().create();
 final Map<String,MilvusClientV2> clients=new HashMap<>();
 final Map<String,Future<Map<String,Object>>> jobs=new HashMap<>();
 final Map<String,Map<String,Integer>> counts=new ConcurrentHashMap<>();
 static final Context.Key<String> LIST_REQUEST_ID = Context.key("list-request-id");
 static final Metadata.Key<String> REQUEST_ID_HEADER =
     Metadata.Key.of("client-request-id", Metadata.ASCII_STRING_MARSHALLER);

 final Map<String, Callable<List<Object>>> iteratorBatches = new HashMap<>();
 final Map<String, Runnable> iteratorClosers = new HashMap<>();
 final ExecutorService pool=Executors.newCachedThreadPool();
 static Map<String,Object> m(Object... kv){Map<String,Object> r=new LinkedHashMap<>();for(int i=0;i<kv.length;i+=2)r.put((String)kv[i],kv[i+1]);return r;}
 static String s(JsonObject c,String k){return c.has(k)&&!c.get(k).isJsonNull()?c.get(k).getAsString():"";}
 static int n(JsonObject c,String k){return c.has(k)?c.get(k).getAsInt():0;}
 static Map<String,Object> error(Throwable e){if(e instanceof ExecutionException)e=e.getCause();Object code=-1;if(e instanceof MilvusClientException){MilvusClientException x=(MilvusClientException)e;code=x.getServerErrCode()!=0?x.getServerErrCode():x.getErrorCode().name();}return m("ok",false,"code",code,"message",e.toString());}
 static List<Map<String,Object>> rows(List<RunningRequestInfo> rs){List<Map<String,Object>> out=new ArrayList<>();for(RunningRequestInfo r:rs)out.add(m("request_id",r.getRequestId(),"proxy_id",r.getProxyId(),"type",r.getType(),"db_name",r.getDatabaseName(),"collection_name",r.getCollectionName(),"user",r.getUser(),"client_addr",r.getClientAddr(),"nq",r.getNq(),"topk",r.getTopK(),"expr",r.getExpr(),"start_time_ms",r.getStartTimeMs(),"queued_ms",r.getQueuedMs(),"elapsed_ms",r.getElapsedMs(),"state",r.getState(),"task_ids",r.getTaskIds(),"trace_id",r.getTraceId(),"cancellable",r.getCancellable()));return out;}
 static List<Map<String,Object>> nodes(List<RunningRequestNodeResult> ns){List<Map<String,Object>> out=new ArrayList<>();for(RunningRequestNodeResult n:ns)out.add(m("node_id",n.getNodeId(),"answered",n.isSuccess(),"unimplemented",n.getUnimplemented(),"error",n.getReason()));return out;}
 static List<BaseVector> readQueryVectors(JsonObject command) throws IOException {
  Path path = Paths.get(s(command, "Vectors"));
  String kind = s(command, "VectorKind");
  int count = n(command, "NQ");
  List<BaseVector> vectors = new ArrayList<>(count);
  if (kind.isEmpty()) {
   ByteBuffer input = ByteBuffer.wrap(Files.readAllBytes(path)).order(ByteOrder.LITTLE_ENDIAN);
   for (int i = 0; i < count; i++) {
    float[] vector = new float[128];
    for (int j = 0; j < vector.length; j++) {
     vector[j] = input.getFloat();
    }
    vectors.add(new FloatVec(vector));
   }
   return vectors;
  }

  JsonArray inputs = JsonParser.parseString(Files.readString(path)).getAsJsonArray();
  if (inputs.size() < count) {
   throw new IllegalArgumentException("Indexed input has fewer than " + count + " vectors");
  }
  for (int i = 0; i < count; i++) {
   JsonElement input = inputs.get(i);
   switch (kind) {
    case "binary":
     byte[] bytes = Base64.getDecoder().decode(input.getAsString());
     if (bytes.length != 16) {
      throw new IllegalArgumentException("Binary input must contain 16 bytes");
     }
     vectors.add(new BinaryVec(bytes));
     break;
    case "sparse":
     SortedMap<Long, Float> weights = new TreeMap<>();
     for (Map.Entry<String, JsonElement> entry : input.getAsJsonObject().entrySet()) {
      weights.put(Long.parseLong(entry.getKey()), entry.getValue().getAsFloat());
     }
     vectors.add(new SparseFloatVec(weights));
     break;
    case "struct-embedding":
     JsonArray embeddings = input.getAsJsonArray();
     if (embeddings.size() != 1 || embeddings.get(0).getAsJsonArray().size() != 32) {
      throw new IllegalArgumentException("Struct input must contain one 32-dimensional embedding");
     }
     EmbeddingList list = new EmbeddingList();
     list.add(new FloatVec(G.fromJson(embeddings.get(0), float[].class)));
     vectors.add(list);
     break;
    case "bm25":
     vectors.add(new EmbeddedText(input.getAsString()));
     break;
    default:
     throw new IllegalArgumentException("Unknown vector kind " + kind);
   }
  }
  return vectors;
 }

 Map<String,Object> query(JsonObject c){try{
  MilvusClientV2 x=clients.get(s(c,"Client"));String col=s(c,"Collection"),expr=s(c,"Expr");int limit=n(c,"Limit");
  if (s(c, "Kind").equals("Query")) {
   List<String> fields = Arrays.asList("id", "vector");
   if (c.has("OutputFields")) {
    fields = new ArrayList<>();
    for (JsonElement field : c.getAsJsonArray("OutputFields")) {
     fields.add(field.getAsString());
    }
   }
   QueryResp response = x.query(QueryReq.builder().collectionName(col).filter(expr)
       .outputFields(fields).limit((long) limit).build());
   Map<String, Object> result = m("ok", true, "count", response.getQueryResults().size());
   if (c.has("ReturnQueryRows") && c.get("ReturnQueryRows").getAsBoolean()) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (QueryResp.QueryResult row : response.getQueryResults()) {
     rows.add(row.getEntity());
    }
    result.put("rows", rows);
   }
   return result;
  }
  List<BaseVector> vs = readQueryVectors(c);
  SearchResp r;
  if(s(c,"Kind").equals("HybridSearch")){AnnSearchReq a=AnnSearchReq.builder().vectorFieldName("vector").vectors(vs).filter(expr).limit(limit).build();r=x.hybridSearch(HybridSearchReq.builder().collectionName(col).searchRequests(Arrays.asList(a,a)).ranker(RRFRanker.builder().build()).limit(limit).build());}
  else {
   String annsField = s(c, "AnnsField");
   SearchReq.SearchReqBuilder builder = SearchReq.builder().collectionName(col)
       .annsField(annsField.isEmpty() ? "vector" : annsField).data(vs).filter(expr).limit(limit);
   if (c.has("OutputFields")) {
    List<String> fields = new ArrayList<>();
    for (JsonElement field : c.getAsJsonArray("OutputFields")) {
     fields.add(field.getAsString());
    }
    builder.outputFields(fields);
   }
   if (s(c, "VectorKind").equals("struct-embedding")) {
    builder.metricType(IndexParam.MetricType.MAX_SIM_COSINE);
    builder.searchParams(new HashMap<>(Collections.singletonMap("ef", 64)));
   }
   int nprobe = n(c, "NProbe");
   if (nprobe > 0) {
    Map<String, Object> params = new HashMap<>();
    params.put("nprobe", nprobe);
    builder.searchParams(params);
   }
   r = x.search(builder.build());
  }
  List<List<Map<String, Object>>> hits = new ArrayList<>();
  for (List<SearchResp.SearchResult> batch : r.getSearchResults()) {
   List<Map<String, Object>> results = new ArrayList<>();
   for (SearchResp.SearchResult hit : batch) {
    Map<String, Object> entry = m("id", hit.getId(), "distance", hit.getScore());
    if (s(c, "Kind").equals("Search") && c.has("OutputFields")) {
     entry.put("entity", hit.getEntity());
    }
    results.add(entry);
   }
   hits.add(results);
  }
  return m("ok", true, "hits", hits);
 }catch(Throwable e){return error(e);}}
 void openIterator(MilvusClientV2 client, JsonObject c) throws IOException {
  String id = s(c, "Call");
  String collection = s(c, "Collection");
  String expr = s(c, "Expr");
  int size = n(c, "BatchSize");
  int limit = n(c, "Limit");
  if (s(c, "Kind").equals("Search")) {
   SearchIteratorV2 iterator = client.searchIteratorV2(SearchIteratorReqV2.builder()
       .collectionName(collection).vectorFieldName("vector").vectors(readQueryVectors(c))
       .metricType(IndexParam.MetricType.L2).filter(expr).batchSize(size).limit(limit).build());
   iteratorClosers.put(id, iterator::close);
   iteratorBatches.put(id, () -> {
    List<Object> ids = new ArrayList<>();
    for (SearchResp.SearchResult hit : iterator.next()) {
     ids.add(hit.getId());
    }
    return ids;
   });
  } else if (s(c, "Kind").equals("Query")) {
   QueryIterator iterator = client.queryIterator(QueryIteratorReq.builder()
       .collectionName(collection).expr(expr).outputFields(Arrays.asList("id", "vector"))
       .batchSize(size).limit(limit).build());
   iteratorClosers.put(id, iterator::close);
   iteratorBatches.put(id, () -> {
    List<Object> ids = new ArrayList<>();
    for (QueryResultsWrapper.RowRecord row : iterator.next()) {
     ids.add(row.get("id"));
    }
    return ids;
   });
  } else {
   throw new IllegalArgumentException("Unknown iterator kind " + s(c, "Kind"));
  }
 }

 Map<String,Object> run(JsonObject c)throws Exception{
  String name=s(c,"Client");MilvusClientV2 x=clients.get(name);
  switch(s(c,"Op")){
  case "connect":
   // Default database is implicit, as in Python. An explicit default triggers
   // Java's ListDatabases visibility check and rejects a no-privilege observer.
   String db=s(c,"DB");
   x=new MilvusClientV2(ConnectConfig.builder().uri(s(c,"URI")).token(s(c,"Token")).dbName(db.isEmpty()||db.equals("default")?null:db).rpcDeadlineMs(120000).build());clients.put(name,x);counts.put(name,new ConcurrentHashMap<>());
   // Preserve the SDK's channel/auth and count actual generated-stub calls.
   Field f=MilvusClientV2.class.getDeclaredField("blockingStub");f.setAccessible(true);MilvusServiceGrpc.MilvusServiceBlockingStub stub=(MilvusServiceGrpc.MilvusServiceBlockingStub)f.get(x);
   x.setBlockingStub(stub.withInterceptors(new ClientInterceptor() {
    public <A,B> ClientCall<A,B> interceptCall(MethodDescriptor<A,B> method, CallOptions opts, Channel next) {
     counts.get(name).merge(method.getBareMethodName(), 1, Integer::sum);
     ClientCall<A,B> call = next.newCall(method, opts);
     String requestId = LIST_REQUEST_ID.get();
     if (requestId == null || requestId.isEmpty() || !"ListRunningRequests".equals(method.getBareMethodName())) {
      return call;
     }
     return new ForwardingClientCall.SimpleForwardingClientCall<A,B>(call) {
      @Override
      public void start(ClientCall.Listener<B> listener, Metadata headers) {
       headers.put(REQUEST_ID_HEADER, requestId);
       super.start(listener, headers);
      }
     };
    }
   }));
   break;
  case "list": {
   Context scoped = Context.current().withValue(LIST_REQUEST_ID, s(c, "RequestID"));
   Context previous = scoped.attach();
   try {
    ListRunningRequestsReq request = ListRunningRequestsReq.builder()
        .databaseName(s(c, "DB")).collectionName(s(c, "Collection")).user(s(c, "User"))
        .minElapsedMs(c.has("MinMS") ? c.get("MinMS").getAsLong() : 0L).build();
    ListRunningRequestsResp response = x.listRunningRequests(request);
    return m("ok", true, "requests", rows(response.getRequests()), "nodes", nodes(response.getNodeResults()));
   } finally {
    scoped.detach(previous);
   }
  }
  case "cancel":
   List<String> ids=null;if(c.has("IDs")&&!c.get("IDs").isJsonNull()){ids=new ArrayList<>();for(JsonElement id:c.getAsJsonArray("IDs"))ids.add(id.getAsString());}
   CancelRequestsResp r=x.cancelRequests(CancelRequestsReq.builder().requestIds(ids).reason(s(c,"Reason")).user(s(c,"User")).build());return m("ok",true,"canceled",rows(r.getCanceled()),"not_found",r.getNotFound(),"undetermined",r.getUndetermined(),"nodes",nodes(r.getNodeResults()));
  case "iterator-open":
   openIterator(x, c);
   break;
  case "iterator-next": {
   Callable<List<Object>> batch = iteratorBatches.get(s(c, "RequestID"));
   if (batch == null) {
    throw new IllegalArgumentException("Unknown iterator " + s(c, "RequestID"));
   }
   jobs.put(s(c, "Call"), pool.submit(() -> {
    try {
     return m("ok", true, "ids", batch.call());
    } catch (Exception e) {
     return error(e);
    }
   }));
   break;
  }
  case "iterator-close": {
   String iterator = s(c, "RequestID");
   Runnable close = iteratorClosers.remove(iterator);
   iteratorBatches.remove(iterator);
   if (close != null) close.run();
   break;
  }
  case "start":jobs.put(s(c,"Call"),pool.submit(()->query(c)));break;
  case "await":return jobs.get(s(c,"Call")).get(130,TimeUnit.SECONDS);
  case "poll":Future<Map<String,Object>> job=jobs.get(s(c,"Call"));return m("ok",true,"done",job.isDone(),"result",job.isDone()?job.get():null);
  case "counts":return m("ok",true,"counts",counts.get(name));
  default:throw new IllegalArgumentException("Unknown operation");
  }return m("ok",true);
 }
 public static void main(String[] args)throws Exception{Worker w=new Worker();try(BufferedReader in=new BufferedReader(new InputStreamReader(System.in))){String line;while((line=in.readLine())!=null){Map<String,Object> result;try{result=w.run(JsonParser.parseString(line).getAsJsonObject());}catch(Throwable e){result=error(e);}System.out.println(G.toJson(result));}}finally{for(MilvusClientV2 c:w.clients.values())c.close();w.pool.shutdownNow();}}
}
