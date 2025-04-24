import pytest

from base.client_v2_base import TestMilvusClientV2Base
from common.common_type import CaseLabel
from common.text_generator import generate_text_by_analyzer


class TestMilvusClientAnalyzer(TestMilvusClientV2Base):
    analyzer_params_list = [
        {
            "tokenizer": "standard",
            "filter": [
                {
                    "type": "stop",
                    "stop_words": ["is", "the", "this", "a", "an", "and", "or"],
                }
            ],
        },
        {
            "tokenizer": "jieba",
        },
        {
            "tokenizer": "icu"
        }
        # {
        #     "tokenizer": {"type": "lindera", "dict_kind": "ipadic"},
        #     "filter": [
        #         {
        #             "type": "stop",
        #             "stop_words": ["は", "が", "の", "に", "を", "で", "と", "た"],
        #         }
        #     ],
        # },
        # {"tokenizer": {"type": "lindera", "dict_kind": "ko-dic"}},
        # {"tokenizer": {"type": "lindera", "dict_kind": "cc-cedict"}},
    ]
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("analyzer_params", analyzer_params_list)
    def test_analyzer(self, analyzer_params):
        """
        target: test analyzer
        method: use different analyzer params, then run analyzer to get the tokens
        expected: verify the tokens
        """
        client = self._client()
        text = generate_text_by_analyzer(analyzer_params)
        res, _ = self.run_analyzer(client, text, analyzer_params, with_detail=True, with_hash=True)
        res_2, _ = self.run_analyzer(client, text, analyzer_params, with_detail=True, with_hash=True)
        # verify the result are the same when run analyzer twice
        for i in range(len(res.tokens)):
            assert res.tokens[i]["token"] == res_2.tokens[i]["token"]
            assert res.tokens[i]["hash"] == res_2.tokens[i]["hash"]
            assert res.tokens[i]["start_offset"] == res_2.tokens[i]["start_offset"]
            assert res.tokens[i]["end_offset"] == res_2.tokens[i]["end_offset"]
            assert res.tokens[i]["position"] == res_2.tokens[i]["position"]
            assert res.tokens[i]["position_length"] == res_2.tokens[i]["position_length"]

        tokens = res.tokens
        token_list = [r["token"] for r in tokens]
        # Check tokens are not empty
        assert len(token_list) > 0, "No tokens were generated"

        # Check tokens are related to input text (all token should be a substring of the text)
        assert all(
            token.lower() in text.lower() for token in token_list
        ), "some of the tokens do not appear in the original text"

        if "filter" in analyzer_params:
            for filter in analyzer_params["filter"]:
                if filter["type"] == "stop":
                    stop_words = filter["stop_words"]
                    assert not any(
                        token in stop_words for token in tokens
                    ), "some of the tokens are stop words"

        # Check hash value and detail
        for r in res.tokens:
            assert isinstance(r["hash"], int)
            assert isinstance(r["start_offset"], int)
            assert isinstance(r["end_offset"], int)
            assert isinstance(r["position"], int)
            assert isinstance(r["position_length"], int)
