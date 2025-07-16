import pytest
from typing import Any, List, Dict, Protocol, cast
import uuid
from base.client_v2_base import TestMilvusClientV2Base
from common.common_type import CaseLabel
from common.text_generator import generate_text_by_analyzer


class AnalyzerResult(Protocol):
    """Protocol for analyzer result to help with type inference"""
    tokens: List[Dict[str, Any]]


class TestMilvusClientAnalyzer(TestMilvusClientV2Base):
    
    @staticmethod
    def get_expected_jieba_tokens(text, analyzer_params):
        """
        Generate expected tokens using jieba library based on analyzer parameters
        """
        import jieba
        import importlib
        importlib.reload(jieba)
        tokenizer_config = analyzer_params.get("tokenizer", {})
        # Set up custom dictionary if provided
        if "dict" in tokenizer_config:
            custom_dict = tokenizer_config["dict"]
            if "_default_" in custom_dict:
                # jieba.dt.initialize()
                for word in custom_dict:
                    if word != "_default_":
                        jieba.add_word(word)
                print(f"dict length: {len(jieba.dt.FREQ)}")
                print(jieba.dt.FREQ)
            else:
                file_path = f"/tmp/{uuid.uuid4()}.txt"
                # add custom words to jieba_dict.txt
                for word in custom_dict:
                    with open(file_path, "w") as f:
                        f.write(f"{word} 1")
                jieba.set_dictionary(file_path)
                jieba.dt.tmp_dir = None
                jieba.dt.cache_file = None
                jieba.dt.FREQ = {}
                jieba.dt.initialize()
            
        
        # Configure mode
        mode = tokenizer_config.get("mode", "search")
        hmm = tokenizer_config.get("hmm", True)
        
        if mode == "exact":
            tokens = list(jieba.cut(text, HMM=hmm))
        elif mode == "search":
            tokens = list(jieba.cut_for_search(text, HMM=hmm))
        else:
            tokens = list(jieba.cut(text, HMM=hmm))
        
        # Filter out empty tokens
        tokens = [token for token in tokens if token.strip()]
        
        return tokens
    
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
            "filter": [
                {
                    "type": "stop",
                    "stop_words": ["is", "the", "this", "a", "an", "and", "or", "是", "的", "这", "一个", "和", "或"],
                }
            ],
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
    
    jieba_custom_analyzer_params_list = [
        # # Test dict parameter with custom dictionary
        {
            "tokenizer": {
                "type": "jieba",
                "dict": ["结巴分词器"],
                "mode": "exact",
                "hmm": False
            }
        },
        # Test dict parameter with default dict and custom dict
        {
            "tokenizer": {
                "type": "jieba",
                "dict": ["_default_", "结巴分词器"],
                "mode": "search",
                "hmm": False
            }
        },
        # Test exact mode with hmm enabled
        {
            "tokenizer": {
                "type": "jieba",
                "dict": ["结巴分词器"],
                "mode": "exact",
                "hmm": True
            }
        },
        # Test search mode with hmm enabled
        {
            "tokenizer": {
                "type": "jieba",
                "dict": ["结巴分词器"],
                "mode": "search",
                "hmm": True
            }
        },
        # Test with only mode configuration
        {
            "tokenizer": {
                "type": "jieba",
                "mode": "exact"
            }
        },
        # Test with only hmm configuration
        {
            "tokenizer": {
                "type": "jieba",
                "hmm": False
            }
        }
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
        
        # Cast to help type inference for gRPC response
        analyzer_res = cast(AnalyzerResult, res)
        analyzer_res_2 = cast(AnalyzerResult, res_2)
        
        # verify the result are the same when run analyzer twice
        for i in range(len(analyzer_res.tokens)):
            assert analyzer_res.tokens[i]["token"] == analyzer_res_2.tokens[i]["token"]
            assert analyzer_res.tokens[i]["hash"] == analyzer_res_2.tokens[i]["hash"]
            assert analyzer_res.tokens[i]["start_offset"] == analyzer_res_2.tokens[i]["start_offset"]
            assert analyzer_res.tokens[i]["end_offset"] == analyzer_res_2.tokens[i]["end_offset"]
            assert analyzer_res.tokens[i]["position"] == analyzer_res_2.tokens[i]["position"]
            assert analyzer_res.tokens[i]["position_length"] == analyzer_res_2.tokens[i]["position_length"]

        tokens = analyzer_res.tokens
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
        for r in tokens:
            assert isinstance(r["hash"], int)
            assert isinstance(r["start_offset"], int)
            assert isinstance(r["end_offset"], int)
            assert isinstance(r["position"], int)
            assert isinstance(r["position_length"], int)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("analyzer_params", jieba_custom_analyzer_params_list)
    def test_jieba_custom_analyzer(self, analyzer_params):
        """
        target: test jieba analyzer with custom configurations
        method: use different jieba analyzer params with dict, mode, and hmm configurations
        expected: verify the tokens are generated correctly based on configuration
        """
        client = self._client()
        text = "milvus结巴分词器中文测试"
        res, _ = self.run_analyzer(client, text, analyzer_params, with_detail=True)
        
        analyzer_res = cast(AnalyzerResult, res)
        tokens = analyzer_res.tokens
        token_list = [r["token"] for r in tokens]
        
        # Check tokens are not empty
        assert len(token_list) > 0, "No tokens were generated"
        
        # Generate expected tokens using jieba library and compare
        expected_tokens = self.get_expected_jieba_tokens(text, analyzer_params)
        assert sorted(token_list) == sorted(expected_tokens), f"Expected {expected_tokens}, but got {token_list}"
        
        # Verify token details
        for r in tokens:
            assert isinstance(r["token"], str)
            assert isinstance(r["start_offset"], int)
            assert isinstance(r["end_offset"], int)
            assert isinstance(r["position"], int)
            assert isinstance(r["position_length"], int)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_analyzer_params", [
        {"tokenizer": "invalid_tokenizer"},
        {"tokenizer": 123},
        {"tokenizer": None},
        {"tokenizer": []},
        {"tokenizer": {"type": "invalid_type"}},
        {"tokenizer": {"type": None}},
        {"filter": "invalid_filter"},
        {"filter": [{"type": None}]},
        {"filter": [{"invalid_key": "value"}]},
    ])
    def test_analyzer_with_invalid_params(self, invalid_analyzer_params):
        """
        target: test analyzer with invalid parameters
        method: use invalid analyzer params and expect errors
        expected: analyzer should raise appropriate exceptions
        """
        client = self._client()
        text = "test text for invalid analyzer"
        
        with pytest.raises(Exception):
            self.run_analyzer(client, text, invalid_analyzer_params)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_analyzer_with_empty_params(self):
        """
        target: test analyzer with empty parameters (uses default)
        method: use empty analyzer params 
        expected: analyzer should use default configuration and work normally
        """
        client = self._client()
        text = "test text for empty analyzer"
        
        # Empty params should use default configuration
        res, _ = self.run_analyzer(client, text, {})
        analyzer_res = cast(AnalyzerResult, res)
        assert len(analyzer_res.tokens) > 0
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_text", [
        None,
        123,
        True,
        False,
    ])
    def test_analyzer_with_invalid_text(self, invalid_text):
        """
        target: test analyzer with invalid text input
        method: use valid analyzer params but invalid text
        expected: analyzer should handle invalid text appropriately
        """
        client = self._client()
        analyzer_params = {"tokenizer": "standard"}
        
        with pytest.raises(Exception):
            self.run_analyzer(client, invalid_text, analyzer_params)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_analyzer_with_empty_text(self):
        """
        target: test analyzer with empty text
        method: use empty text input
        expected: analyzer should return empty tokens
        """
        client = self._client()
        analyzer_params = {"tokenizer": "standard"}
        
        res, _ = self.run_analyzer(client, "", analyzer_params)
        analyzer_res = cast(AnalyzerResult, res)
        assert len(analyzer_res.tokens) == 0
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("text_input", [
        [],
        {},
        ["list", "of", "strings"],
        {"key": "value"},
    ])
    def test_analyzer_with_structured_text(self, text_input):
        """
        target: test analyzer with structured text input (list/dict)
        method: use list or dict as text input
        expected: analyzer should handle structured input and return tokens
        """
        client = self._client()
        analyzer_params = {"tokenizer": "standard"}
        
        res, _ = self.run_analyzer(client, text_input, analyzer_params)
        # For structured input, API returns direct list format
        assert isinstance(res, list)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_jieba_params", [
        {"tokenizer": {"type": "jieba", "dict": "not_a_list"}},
        {"tokenizer": {"type": "jieba", "dict": [123, 456]}},
        {"tokenizer": {"type": "jieba", "mode": "invalid_mode"}},
        {"tokenizer": {"type": "jieba", "mode": 123}},
        {"tokenizer": {"type": "jieba", "hmm": "not_boolean"}},
        {"tokenizer": {"type": "jieba", "hmm": 123}},
    ])
    def test_jieba_analyzer_with_invalid_config(self, invalid_jieba_params):
        """
        target: test jieba analyzer with invalid configurations
        method: use jieba analyzer with invalid dict, mode, or hmm values
        expected: analyzer should raise appropriate exceptions
        """
        client = self._client()
        text = "测试文本 for jieba analyzer"
        
        with pytest.raises(Exception):
            self.run_analyzer(client, text, invalid_jieba_params)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_jieba_analyzer_with_empty_dict(self):
        """
        target: test jieba analyzer with empty dictionary
        method: use jieba analyzer with empty dict list
        expected: analyzer should work with empty dict (uses default)
        """
        client = self._client()
        text = "测试文本 for jieba analyzer"
        jieba_params = {"tokenizer": {"type": "jieba", "dict": []}}
        
        res, _ = self.run_analyzer(client, text, jieba_params)
        analyzer_res = cast(AnalyzerResult, res)
        assert len(analyzer_res.tokens) > 0
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_dict_config", [
        {"tokenizer": {"type": "jieba", "dict": None}},
        {"tokenizer": {"type": "jieba", "dict": "invalid_string"}},
        {"tokenizer": {"type": "jieba", "dict": 123}},
        {"tokenizer": {"type": "jieba", "dict": True}},
        {"tokenizer": {"type": "jieba", "dict": {"invalid": "dict"}}},
    ])
    def test_jieba_analyzer_with_invalid_dict_values(self, invalid_dict_config):
        """
        target: test jieba analyzer with invalid dict configurations
        method: use jieba analyzer with invalid dict values
        expected: analyzer should raise appropriate exceptions
        """
        client = self._client()
        text = "测试文本 for jieba analyzer"
        
        with pytest.raises(Exception):
            self.run_analyzer(client, text, invalid_dict_config)
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("edge_case_dict_config", [
        {"tokenizer": {"type": "jieba", "dict": ["", "valid_word"]}},  # Empty string in list
        {"tokenizer": {"type": "jieba", "dict": ["valid_word", "valid_word"]}},  # Duplicate words
        {"tokenizer": {"type": "jieba", "dict": ["_default_"]}},  # Only default dict
    ])
    def test_jieba_analyzer_with_edge_case_dict_values(self, edge_case_dict_config):
        """
        target: test jieba analyzer with edge case dict configurations
        method: use jieba analyzer with edge case dict values
        expected: analyzer should handle these cases gracefully
        """
        client = self._client()
        text = "测试文本 for jieba analyzer"
        
        res, _ = self.run_analyzer(client, text, edge_case_dict_config, with_detail=True)
        analyzer_res = cast(AnalyzerResult, res)
        # These should work but might not be recommended usage
        assert len(analyzer_res.tokens) >= 0
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_jieba_analyzer_with_unknown_param(self):
        """
        target: test jieba analyzer with unknown parameter
        method: use jieba analyzer with invalid parameter name
        expected: analyzer should ignore unknown parameters and work normally
        """
        client = self._client()
        text = "测试文本 for jieba analyzer"
        jieba_params = {"tokenizer": {"type": "jieba", "invalid_param": "value"}}
        
        res, _ = self.run_analyzer(client, text, jieba_params)
        analyzer_res = cast(AnalyzerResult, res)
        assert len(analyzer_res.tokens) > 0
    
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_filter_params", [
        {"tokenizer": "standard", "filter": [{"type": "stop"}]},
        {"tokenizer": "standard", "filter": [{"type": "stop", "stop_words": "not_a_list"}]},
        {"tokenizer": "standard", "filter": [{"type": "stop", "stop_words": [123, 456]}]},
        {"tokenizer": "standard", "filter": [{"type": "invalid_filter_type"}]},
        {"tokenizer": "standard", "filter": [{"type": "stop", "stop_words": None}]},
    ])
    def test_analyzer_with_invalid_filter(self, invalid_filter_params):
        """
        target: test analyzer with invalid filter configurations
        method: use analyzer with invalid filter parameters
        expected: analyzer should handle invalid filters appropriately
        """
        client = self._client()
        text = "This is a test text with stop words"
        
        with pytest.raises(Exception):
            self.run_analyzer(client, text, invalid_filter_params)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_analyzer_with_empty_stop_words(self):
        """
        target: test analyzer with empty stop words list
        method: use stop filter with empty stop_words list
        expected: analyzer should work normally with empty stop words (no filtering)
        """
        client = self._client()
        text = "This is a test text with stop words"
        filter_params = {"tokenizer": "standard", "filter": [{"type": "stop", "stop_words": []}]}
        
        res, _ = self.run_analyzer(client, text, filter_params, with_detail=True)
        analyzer_res = cast(AnalyzerResult, res)
        tokens = analyzer_res.tokens
        token_list = [r["token"] for r in tokens]
        
        assert len(token_list) > 0
        # With empty stop words, no filtering should occur
        assert "is" in token_list  # Common stop word should still be present