#!python
import random
import copy

def show_dsl(query_entities):
    if not isinstance(query_entities, (dict,)):
        raise ParamError("Invalid query format. 'query_entities' must be a dict")

    duplicated_entities = copy.deepcopy(query_entities)
    vector_placeholders = dict()

    def extract_vectors_param(param, placeholders):
        if not isinstance(param, (dict, list)):
            return

        if isinstance(param, dict):
            if "vector" in param:
                # TODO: Here may not replace ph
                ph = "$" + str(len(placeholders))

                for pk, pv in param["vector"].items():
                    if "query" not in pv:
                        raise ParamError("param vector must contain 'query'")
                    placeholders[ph] = pv["query"]
                    param["vector"][pk]["query"] = ph

                return
            else:
                for _, v in param.items():
                    extract_vectors_param(v, placeholders)

        if isinstance(param, list):
            for item in param:
                extract_vectors_param(item, placeholders)

    extract_vectors_param(duplicated_entities, vector_placeholders)
    print(duplicated_entities)

    for tag, vectors in vector_placeholders.items():
        print("tag: ", tag)

if __name__ == "__main__":
    num = 5
    dimension = 4
    vectors = [[random.random() for _ in range(4)] for _ in range(num)]
    dsl = {
        "bool": {
            "must":[
                {
                    "term": {"A": [1, 2, 5]}
                },
                {
                    "range": {"B": {"GT": 1, "LT": 100}}
                },
                {
                    "vector": {
                       "Vec": {"topk": 10, "query": vectors[:1], "metric_type": "L2", "params": {"nprobe": 10}}
                    }
                }
            ]
        }
    }
    show_dsl(dsl)

