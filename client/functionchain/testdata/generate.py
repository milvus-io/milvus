# Generated against PyMilvus commit 0e1fff92cf0c8d550aef8c1ad3cbc89a231e1473.
from pathlib import Path
from google.protobuf.json_format import MessageToJson
from pymilvus.function_chain import FunctionChain as C, FunctionChainStage as S, FunctionChainExpr, col, fn
chains = {
'rerank': C(S.L2_RERANK,'fresh_popular').map('freshness',fn.decay(col('published_at'),function='exp',origin=1700000000,scale=86400)).map('$score',fn.num_combine(col('$score'),col('freshness'),mode='weighted',weights=[0.7,0.3])).map('$score',fn.round_decimal(col('$score'),decimal=4)).sort(col('$score'),tie_break_col=col('$id')).limit(10,2),
'grep': C(S.QUERY_SCAN,'grep',output_columns=['matches']).grep('content',arguments="-i -e 'timeout'"),
'rrf': C(S.L2_RERANK).merge('rrf',weights=[0.7,0.3]),
'weighted': C(S.L2_RERANK).merge('weighted',weights=[0.7,0.3]),
'xgboost': C(S.L0_RERANK).map('$score',fn.xgboost(col('price'),col('popularity'),model_resource='model',output='raw')),
'model': C(S.L2_RERANK).map('$score',fn.rerank_model(col('text'),queries=['query'],provider='voyageai')),
'literals': C(S.POST_PROCESS).map('out',FunctionChainExpr('custom',args=(col('text'),'literal',True,-12,1.25,bytes([0,255]),[1,'x'],{'nested':[True,False]}))),
}
for name,c in chains.items():
 Path(__file__).with_name(name+'.json').write_text(MessageToJson(c.to_proto())+'\n')
