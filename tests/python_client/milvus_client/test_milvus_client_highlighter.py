from itertools import cycle
import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from pymilvus import Function, FunctionType, DataType, LexicalHighlighter

prefix = "client_insert"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_text_field_name = "tr"
default_text_field_name_chinese = "tr_chinese"
default_text_field_name_no_BM25 = "tr_no_bm25"
default_text_field_name_multi_analyzer = "tr_multi_analyzer"
default_sparse_vector_field_name = f"{default_text_field_name}_sparse_emb"
default_sparse_vector_field_name_chinese = f"{default_text_field_name_chinese}_sparse_emb"
default_sparse_vector_field_name_multi_analyzer = f"{default_text_field_name_multi_analyzer}_sparse_emb"

COLLECTION_NAME = "test_hightligher"
@pytest.mark.xdist_group("TestMilvusClientHighlighter")
class TestMilvusClientHighlighter(TestMilvusClientV2Base):
    """
    #########################################################
    Init collection with highlighter so all the tests can use the same collection
    This aims to save time for the tests
    Also, highlighter is difficult to compare the results,
    so we need to init the collection with pre-defined data
    #########################################################
    """
    @pytest.fixture(scope="module", autouse=True)
    def prepare_highlighter_collection(self, request):
        """
        Ensure the shared highlighter collection exists before any tests in this module,
        and drop it after all tests in this module complete.
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        if client.has_collection(collection_name):
            client.drop_collection(collection_name)
        
        analyzer_params = {
            "tokenizer": "standard"
        }
        analyzer_params_2 = {
            "tokenizer": {
                "type": "jieba",
                "dict": ["结巴分词器"],
                "mode": "exact",
                "hmm": False
            }
        }
        multi_analyzer_params = {
            "by_field": "language",
            "analyzers": {
                "en": {"type": "english"},
                "zh": {"type": "chinese"},
                "default": {"tokenizer": "icu"},
            },
            "alias": {"chinese": "zh", "eng": "en"},
        }
        schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
        schema.add_field(field_name=default_primary_key_field_name, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=default_vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="language", datatype=DataType.VARCHAR, max_length=16)
        schema.add_field(field_name=default_text_field_name, datatype=DataType.VARCHAR, nullable=True, max_length=2000, enable_analyzer=True,
                    analyzer_params=analyzer_params)
        schema.add_field(field_name=default_text_field_name_chinese, datatype=DataType.VARCHAR, nullable=True, max_length=2000, enable_analyzer=True,
                    analyzer_params=analyzer_params_2)
        schema.add_field(field_name=default_text_field_name_no_BM25, datatype=DataType.VARCHAR, nullable=True, max_length=2000, enable_analyzer=True,
                    analyzer_params=analyzer_params_2)
        schema.add_field(field_name=default_text_field_name_multi_analyzer, datatype=DataType.VARCHAR, nullable=True, max_length=2000, enable_analyzer=True,
                    multi_analyzer_params=multi_analyzer_params)
        schema.add_field(field_name=default_sparse_vector_field_name, datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field(field_name=default_sparse_vector_field_name_chinese, datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field(field_name=default_sparse_vector_field_name_multi_analyzer, datatype=DataType.SPARSE_FLOAT_VECTOR)
        bm25_function = Function(
            name=f"{default_text_field_name}_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=[default_text_field_name],
            output_field_names=[default_sparse_vector_field_name],
            params={}
        )
        bm25_function_2 = Function(
            name=f"{default_text_field_name_chinese}_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=[default_text_field_name_chinese],
            output_field_names=[default_sparse_vector_field_name_chinese],
            params={}
        )
        bm25_function_multi_analyzer = Function(
            name=f"{default_text_field_name_multi_analyzer}_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=[default_text_field_name_multi_analyzer],
            output_field_names=[default_sparse_vector_field_name_multi_analyzer],
            params={}
        )
        schema.add_function(bm25_function)
        schema.add_function(bm25_function_2)
        schema.add_function(bm25_function_multi_analyzer)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name=default_primary_key_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=default_vector_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=default_sparse_vector_field_name, index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        index_params.add_index(field_name=default_sparse_vector_field_name_chinese, index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        index_params.add_index(field_name=default_sparse_vector_field_name_multi_analyzer, index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        index_params.add_index(field_name=default_text_field_name, index_type="AUTOINDEX")
        index_params.add_index(field_name=default_text_field_name_chinese, index_type="AUTOINDEX")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params, consistency_level="Strong")

        text = ["Is there a leakage?",
                "A leakage of what?",
                "I have the seat full of water! Like, full of water!",
                "Must be water.",
                "Let's add that to the words of wisdom",
                "7654321 keyword 1234567",
                "key key key key key key key key",
                "trytrytrytrytrytry",
                "word 1 word 12 word 123",
                "1 2 3 4 5 6 7 8 9 10",
                "A B C D 一二三四 milvus结巴分词器中文测试",
                "There is a sub-word in this sentence",
                "",
                None,
                ("Dusk settled gently upon Embermoor, turning streets into ribbons in twilight glow. "
                "In this quiet district, young Teren roamed with restless intent. He sought purpose, "
                "something bright enough to hush turmoil pressing within his chest.\n"
                "Teren’s trouble began when curious murmurs drifted through town concerning shimmering "
                "lights rising nightly beyond hills. Elders insisted it stemmed from old ruins, relics "
                "left behind by wanderers long gone. Yet no one ventured there; timid hearts kept them "
                "grounded.\n"
                "One evening, driven by stubborn courage, Teren set out alone. Crisp wind brushed his "
                "cheeks, guiding him through slender trees trembling under midnight hush. Crickets chirped "
                "rhythmically, echoing his steady footsteps. He pressed forward until dim ruins emerged, "
                "stones bent by centuries yet proud in their quiet endurance.\n"
                "Upon entering, Teren sensed something stirring—bright pulses drifting through corridors "
                "like living embers. One ember hovered close, swirling gently, studying him with earnest "
                "curiosity. It emitted tender hums resonating deep within Teren’s chest, soothing worry "
                "stitched into his spirit.\n"
                "Ember drifted higher, inviting him to follow. Teren stepped through crumbled chambers "
                "until they reached an inner court where hundreds curled in silent orbits—tiny spheres "
                "burning with soft brilliance. Together they formed swirling constellations, shimmering "
                "Instantly, warmth surged through him—not harsh, not wild, but gentle strength reminding "
                "him he belonged in this immense world. Hidden burdens loosened. He felt courage blooming, "
                "rooted in something deeper than fear.\n"
                "When dawn arrived, Ember escorting him outside, Teren turned to ruins glowing faintly "
                "beneath morning light. He understood now: these spirits lingered not for warning but for "
                "guiding tender souls seeking direction.\n"
                "He returned to Embermoor changed. Not every problem dissolved, yet Teren moved through "
                "his days with renewed stride, carrying brilliance gifted during his midnight journey."),
                ("黄昏降临在静谧城镇，灯影沿着街道缓缓铺展。青年林舟怀着不安在巷道行走，心跳与脚步相互呼应。他渴望找到方向，却被往昔失落缠绕。"
                "传言提到远处丘陵夜晚会浮现微光，长者劝人别靠近，担忧未知带来风险。林舟仍被好奇牵引，选择独自前往。冷风掠过树梢，星辰悬挂高空，陪伴他穿越草径。"
                "残破遗迹映入眼帘，石壁布满岁月痕迹。踏入其内，柔亮光点缓缓旋转，如同呼吸般起伏。某个光点靠近他，散发温暖振动，仿佛聆听他内心低语。"
                "光点引领他走向空旷庭院，成群光辉在空域盘旋，编织出壮丽图景。那瞬间，林舟感到胸口释然，恐惧逐渐消散，勇气悄然生根。"
                "黎明到来，他回望遗迹，光辉渐隐却留存于心。归途上，他步伐坚定，明白指引并未消失，只是化作持续前行力量。每当夜色再度降临，他都会抬头微笑，感谢那段静默旅程。"),
                "甲，甲乙，甲乙丙，甲乙丙丁，甲乙丙丁戊，甲乙丙丁戊己，甲乙丙丁戊己庚，甲乙丙丁戊己庚辛，甲乙丙丁戊己庚辛壬，甲乙丙丁戊己庚辛壬癸"]

        l = len(text)

        rows = cf.gen_row_data_by_schema(nb=l, schema=schema)
        for i, row in enumerate(rows):
            row[default_text_field_name] = text[i]
            row[default_text_field_name_chinese] = text[i]
            row[default_text_field_name_no_BM25] = text[i]
            row[default_text_field_name_multi_analyzer] = text[i]
            row["language"] = "en" if i % 2 == 0 else "zh"

        client.insert(collection_name=collection_name, data=rows)
        
        def teardown():
            try:
                if self.has_collection(self._client(), COLLECTION_NAME):
                    self.drop_collection(self._client(), COLLECTION_NAME)
            except Exception:
                pass
        request.addfinalizer(teardown)

    """
    ******************************************************************
    #  The following are valid test cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_highlighter_basic(self):
        """
        target: Test highlighter can be successfully used
        method:
            1. Search the data
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        pre_tags = ["<<<<<<<"]
        post_tags = [">>>>>>"]
        highlight = LexicalHighlighter(pre_tags=pre_tags, post_tags=post_tags, 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 10,
                               num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}

        expected = [[f"{pre_tags[0]}water{post_tags[0]}."],
                    [f"{pre_tags[0]}water{post_tags[0]}! Lik"]]

        results = client.search(
            collection_name, 
            ["water"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name], 
            highlighter = highlight
        )

        # assert to make sure the results are not empty
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name] in expected


        # test with multi analyzer
        # BUG: #46498
        '''
        results = client.search(
            collection_name, 
            ["water"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name_multi_analyzer,
            output_fields=[default_text_field_name_multi_analyzer], 
            highlighter = highlight
        )
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name_multi_analyzer] in expected
            '''

    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_multiple_tags(self):
        """
        target: Test highlighter can be successfully used with multiple tags
        method:
            1. Search the data
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        pre_tags = ["{", "<", "="]
        post_tags = ["}", ">", "="]
        highlight = LexicalHighlighter(pre_tags=pre_tags, post_tags=post_tags, 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 100,
                               num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}

        expected = []
        for pre, post in cycle(zip(pre_tags, post_tags)):
            expected.append(f"{pre}key{post}")
            if len(expected) == 8:
                break
        expected = " ".join(expected)

        results = client.search(
            collection_name, 
            ["key"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name], 
            highlighter = highlight
        )

        # assert to make sure the results are not empty
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name] == [expected]


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_fragment_parameters(self):
        """
        target: Test highlighter can be successfully used with fragment parameters
        method:
            1. Search the data with different fragment parameters
               nested list: row corresponds to fragment_size, column corresponds to num_of_fragments
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        fragment_size = [1, 9, 100]
        num_of_fragments = [0, 1, 2]
        
        # row 0 corresponds to fragment_size = 1   column 0 corresponds to num_of_fragments = 0
        # row 1 corresponds to fragment_size = 9   column 1 corresponds to num_of_fragments = 1
        # row 2 corresponds to fragment_size = 100 column 2 corresponds to num_of_fragments = 2
        expected = [[[[]], [["{water}"]],                                       [['{water}'], ["{water}", "{water}"]]],
                    [[[]], [['{water}.'], ['{water}! Li']],                     [['{water}.'], ['{water}! Li', '{water}!']]],
                    [[[]], [['{water}.'], ['{water}! Like, full of {water}!']], [['{water}.'], ['{water}! Like, full of {water}!']]]]
       
        for i, size in enumerate(fragment_size):
            for j, num in enumerate(num_of_fragments):
                highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = size,
                               num_of_fragments=num)
                search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
                results = client.search(
                    collection_name, 
                    ["water"],
                    search_params=search_params,
                    anns_field=default_sparse_vector_field_name,
                    output_fields=[default_text_field_name], 
                    highlighter = highlight
                )

                # assert to make sure the results are not empty
                assert results[0] != []
                for result in results[0]:
                    assert result['highlight'][default_text_field_name] in expected[i][j]

    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_fragment_offset(self):
        """
        target: Test highlighter can be successfully used with fragment offset
        method:
            1. Search the data with different fragment offset
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        fragment_offset = [0, 5, 100]

        expected = [[["=water="],         ['=water=', '=water=']],
                    [['t be =water='],    ['l of =water=', 'l of =water=']], 
                    [['Must be =water='], ['I have the seat full of =water=', 'I have the seat full of water! Like, full of =water=']]]

        for i, offset in enumerate(fragment_offset):
            highlight = LexicalHighlighter(pre_tags=["="], post_tags=["="], 
                               highlight_search_text = True, 
                               fragment_offset=offset,
                               fragment_size = 5,
                               num_of_fragments=2)
            search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
            results = client.search(
                collection_name, 
                ["water"],
                search_params=search_params,
                anns_field=default_sparse_vector_field_name,
                output_fields=[default_text_field_name], 
                highlighter = highlight
            )

            # assert to make sure the results are not empty
            assert results[0] != []
            for result in results[0]:
                assert result['highlight'][default_text_field_name] in expected[i]

    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_number(self):
        """
        target: Test highlighter can be successfully used with numbers
        method:
            1. Search the data with different fragment size
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME

        expected = [[['word {1}'], ['{1} 2 3']],
                    [['7654321 keyword {1234567}']]]
        highlight = LexicalHighlighter(pre_tags=["{", "<"], post_tags=["}", ">"], 
                               highlight_search_text = True, 
                               fragment_offset=100, 
                               fragment_size = 5,
                               num_of_fragments=2)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}

        results = client.search(
            collection_name, 
            ["1", "1234567"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name], 
            highlighter = highlight
        )

        for i, result in enumerate(results):
            # assert to make sure the results are not empty
            assert result != []
            for res in result:
                assert res['highlight'][default_text_field_name] in expected[i]
            

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_offset_greater_than_fragment_size(self):
        """
        target: Test highlighter can be successfully used with offset greater than fragment size
        method:
            1. Search the data with offset greater than fragment size
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        expected = ["4321 {keyword}"]
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=5, 
                               fragment_size = 1,
                               num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        results = client.search(
            collection_name, 
            ["keyword"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name],
            highlighter = highlight
        )

        # assert to make sure the results are not empty
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name] == expected

    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_search_sentence(self):
        """
        target: Test highlighter can be successfully used with search sentence
        method:
            1. Search the data with search sentence
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        expected = [['{A} {leakage} {of}', 'e of {what}?'], 
                    ['re a {leakage}'], 
                    ['{A} B C D 一二'], 
                    ['full {of} wa', 'full {of} wa'], 
                    ['ords {of} wi']
                ]
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=5, 
                               fragment_size = 10,
                               num_of_fragments=10)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        results = client.search(
            collection_name, 
            ["A leakage of what?"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name],
            highlighter = highlight
        )

        # assert to make sure the results are not empty
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name] in expected


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_nonexistent_keyword(self):
        """
        target: Test highlighter can be successfully used with nonexistent keyword
        method:
            1. Search the data with nonexistent keyword
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        expected = []
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 1,
                               num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        results = client.search(
            collection_name, 
            ["nonexistent", "", "NULL", "None", "null", 'NaN'],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name],
            highlighter = highlight
        )

        for result in results:
            assert result == expected

    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_sub_word(self):
        """
        target: Test highlighter can be successfully used with sub word
        method:
            1. Search the data with sub word
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        expected = ["{sub}"]
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 1,
                               num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        results = client.search(
            collection_name, 
            ["sub"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name],
            highlighter = highlight
        )

        # assert to make sure the results are not empty
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name] == expected


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_long_tags(self):
        """
        target: Test highlighter can be successfully used with long tags
        method:
            1. Search the data with long tags
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        pre_tags = ["{" * 999]
        post_tags = ["}" * 999]
        expected = [f"{pre_tags[0]}water{post_tags[0]}"]
        highlight = LexicalHighlighter(pre_tags=pre_tags, post_tags=post_tags, 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 1,
                               num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        results = client.search(
            collection_name, 
            ["water"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name],
            highlighter = highlight
        )

        # assert to make sure the results are not empty
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name] == expected


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_long_search_text(self):
        """
        target: Test highlighter can be successfully used with long search text
        method:
            1. Search the data with long search text
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        expected = ["{Embermoor}", "{Embermoor}"]
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 1,
                               num_of_fragments=10)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        results = client.search(
            collection_name, 
            ["Embermoor"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name],
            highlighter = highlight
        )
        # assert to make sure the results are not empty
        print(f"the results are {results}")
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name] == expected


   
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_lower_upper_case(self):
        """
        target: Test highlighter can be successfully used with text match and highlight search text
        method:
            1. Search the data with text match and highlight search text
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 100,
                               num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        results = client.search(
            collection_name, 
            ["WATER"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name],
            highlighter = highlight
        )
        assert results[0] == []


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_chinese_characters(self):
        """
        target: Test highlighter can be successfully used with chinese characters
        method:
            1. Search the data with chinese characters
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        expected = ["{二}"]
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 1,
                               num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}

        results = client.search(
            collection_name, 
            ["二"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name_chinese,
            output_fields=[default_text_field_name_chinese],
            highlighter = highlight
        )

        # assert to make sure the results are not empty
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name_chinese] == expected

        expected = ["{结巴分词器}"]
        results = client.search(
            collection_name, 
            ["结巴分词器"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name_chinese,
            output_fields=[default_text_field_name_chinese],
            highlighter = highlight
        )
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name_chinese] == expected
    

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_chinese_characters_long_text(self):
        """
        target: Test highlighter can be successfully used with chinese characters
        method:
            1. Search the data with chinese characters
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        expected = ["{呼}","{如同呼吸般起伏}"]
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 1,
                               num_of_fragments=10)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}

        results = client.search(
            collection_name, 
            ["如同呼吸般起伏"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name_chinese,
            output_fields=[default_text_field_name_chinese],
            highlighter = highlight
        )
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name_chinese] == expected


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_with_query(self):
        """
        target: Test highlighter can be successfully used with query
        method:
            1. Search the data with query
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        pre_tags = ["<<<<<<<"]
        post_tags = [">>>>>>"]
        highlight = LexicalHighlighter(pre_tags=pre_tags, post_tags=post_tags, 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 10,
                               num_of_fragments=1,
                               queries=[{"type": "TextMatch", "field": default_text_field_name_no_BM25, "text": "seat"}])
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}

        expected = [[f"{pre_tags[0]}water{post_tags[0]}."],
                    [f"{pre_tags[0]}water{post_tags[0]}! Lik"]]

        results = client.search(
            collection_name, 
            ["water"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name], 
            highlighter = highlight
        )

        # assert to make sure the results are not empty
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name] in expected

    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_text_match(self):
        """
        target: Test highlighter can be successfully used with text match
        method:
            1. Search the data with text match
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        expected = ['{water}! Like, full of <water>!']
        
        highlight = LexicalHighlighter(pre_tags=["{", "<"], post_tags=["}", ">"], 
                                    highlight_search_text = False, 
                                    queries=[{"type": "TextMatch", "field": default_text_field_name_no_BM25, "text": "water"}])

        new_search_params = {"metric_type": "COSINE"}


        vector = client.query(collection_name, filter=f"{default_primary_key_field_name} == 2", output_fields=[default_vector_field_name])[0][default_vector_field_name]
        
        results = client.search(
            collection_name,
            [vector],
            search_params=new_search_params,
            anns_field=default_vector_field_name,
            output_fields=[default_text_field_name_no_BM25], 
            limit=1,
            highlighter = highlight
        )

        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name_no_BM25] == expected

        # test with mismatched tags
        expected = ['{water}! Like, full of {water>!']
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}", ">"], 
                                    highlight_search_text = False, 
                                    queries=[{"type": "TextMatch", "field": default_text_field_name_no_BM25, "text": "water"}])

        results = client.search(
            collection_name,
            [vector],
            search_params=new_search_params,
            anns_field=default_vector_field_name,
            output_fields=[default_text_field_name_no_BM25], 
            limit=1,
            highlighter = highlight
        )
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name_no_BM25] == expected

        # test with num_of_fragments > 1
        expected = ['{water}', '{water}']
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                                    highlight_search_text = False, 
                                    fragment_size = 1,
                                    num_of_fragments=10,
                                    queries=[{"type": "TextMatch", "field": default_text_field_name_no_BM25, "text": "water"}])

        results = client.search(
            collection_name,
            [vector],
            search_params=new_search_params,
            anns_field=default_vector_field_name,
            output_fields=[default_text_field_name_no_BM25], 
            limit=1,
            highlighter = highlight
        )
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name_no_BM25] == expected

        # test no match
        expected = []
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                                    highlight_search_text = False, 
                                    queries=[{"type": "TextMatch", "field": default_text_field_name_no_BM25, "text": "nonexistent"}])
        vector = client.query(collection_name, filter=f"{default_primary_key_field_name} == 1", output_fields=[default_vector_field_name])[0][default_vector_field_name]
        results = client.search(
            collection_name,
            [vector],
            search_params=new_search_params,
            anns_field=default_vector_field_name,
            output_fields=[default_text_field_name_no_BM25], 
            limit=1,
            highlighter = highlight
        )
        assert results[0][0]["highlight"][default_text_field_name_no_BM25] == expected


    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="skip for now wait for future optimization")
    def test_milvus_client_highlighter_chinese_characters_repeating_text(self):
        """
        target: Test highlighter can be successfully used with repeating text
        method:
            1. Search the data with repeating text
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        expected = ["{甲乙丙丁戊己庚辛壬}"]
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 1,
                               num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        results = client.search(
            collection_name, 
            ["甲乙丙丁戊己庚辛壬"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name_chinese,
            output_fields=[default_text_field_name_chinese],
            highlighter = highlight
        )
        assert results[0] != []
        for result in results[0]:
            assert result['highlight'][default_text_field_name_chinese] == expected



    """
    ******************************************************************
    #  The following are invalid test cases
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("fragment_size", [0, -1, 0.1])
    def test_milvus_client_highlighter_fragment_size_invalid(self, fragment_size):
        """
        target: Test highlighter can be successfully used with fragment size zero
        method:
            1. Search the data with fragment size zero
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = fragment_size,
                               num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}

        error = {ct.err_code: 1100,
                 ct.err_msg: f"invalid fragment_size: {fragment_size}: invalid parameter"}
        self.search(
            client,
            collection_name, 
            ["water"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name],
            highlighter = highlight,
            check_task=CheckTasks.err_res,
            check_items=error
        )


    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("fragment_offset", [-1, 0.1])
    def test_milvus_client_highlighter_fragment_offset_invalid(self, fragment_offset):
        """
        target: Test highlighter can be successfully used with fragment offset negative
        method:
            1. Search the data with fragment offset negative
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=fragment_offset, 
                               fragment_size = 10,
                               num_of_fragments=10)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        error = {ct.err_code: 1100,
                 ct.err_msg: f"invalid fragment_offset: {fragment_offset}: invalid parameter"}
        self.search(
            client,
            collection_name, 
            ["water"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name],
            highlighter = highlight,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("num_of_fragments", [-1, 0.1])
    def test_milvus_client_highlighter_number_of_fragments_invalid(self, num_of_fragments):
        """
        target: Test highlighter can be successfully used with number of fragments invalid
        method:
            1. Search the data with number of fragments invalid
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 10,
                               num_of_fragments=num_of_fragments)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        error = {ct.err_code: 1100,
                 ct.err_msg: f"invalid num_of_fragments: {num_of_fragments}: invalid parameter"}
        self.search(
            client,
            collection_name, 
            ["water"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name],
            highlighter = highlight,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_text_match_invalid_search_params(self):
        """
        target: Test highlighter can be successfully used with text match invalid search params
        method:
            1. Search the data with text match invalid search params
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        pre_tags = ["<<<<<<<"]
        post_tags = [">>>>>>"]
        highlight = LexicalHighlighter(pre_tags=pre_tags, post_tags=post_tags, 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 10,
                               num_of_fragments=1,
                               queries=[{"type": "TextMatch", "field": default_text_field_name_no_BM25, "text": "seat"}])
        search_params = {"metric_type": "COSINE"}
        vector = client.query(collection_name, filter=f"{default_primary_key_field_name} == 2", output_fields=[default_vector_field_name])[0][default_vector_field_name]

        error = {ct.err_code: 1100,
                 ct.err_msg: f"Search highlight only support with metric type \"BM25\" but was: : invalid parameter"}
        self.search(
            client,
            collection_name, 
            [vector],
            search_params=search_params,
            anns_field=default_vector_field_name,
            output_fields=[default_text_field_name], 
            highlighter = highlight,
            check_task=CheckTasks.err_res,
            check_items=error
        )


    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_text_match_invalid_anns_field(self):
        """
        target: Test highlighter can be successfully used with text match invalid anns field
        method:
            1. Search the data with text match invalid anns field
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        pre_tags = ["<<<<<<<"]
        post_tags = [">>>>>>"]
        highlight = LexicalHighlighter(pre_tags=pre_tags, post_tags=post_tags, 
                               highlight_search_text = True, 
                               fragment_offset=0, 
                               fragment_size = 10,
                               num_of_fragments=1,
                               queries=[{"type": "TextMatch", "field": default_text_field_name_no_BM25, "text": "seat"}])
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        vector = client.query(collection_name, filter=f"{default_primary_key_field_name} == 2", output_fields=[default_vector_field_name])[0][default_vector_field_name]
  

        # textMatch with BM25 anns field
        error = {ct.err_code: 1100,
                 ct.err_msg: f"please provide varchar/text for BM25 Function based search, got FloatVector: invalid parameter"}
        self.search(
            client,
            collection_name, 
            [vector],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name], 
            highlighter = highlight,
            check_task=CheckTasks.err_res,
            check_items=error
        )

        # highlight search text with vector anns field
        error = {ct.err_code: 5,
                 ct.err_msg: f"service internal error: Search with highlight failed, input field of BM25 annsField not found"}
        self.search(
            client,
            collection_name, 
            ["water"],
            search_params=search_params,
            anns_field=default_vector_field_name,
            output_fields=[default_text_field_name], 
            highlighter = highlight,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_text_match_with_highlight_search_text(self):
        """
        target: Test highlighter can be successfully used with text match and highlight search text
        method:
            1. Search the data with text match and highlight search text
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        
        highlight = LexicalHighlighter(pre_tags=["{", "<"], post_tags=["}", ">"], 
                                    highlight_search_text = True, 
                                    queries=[{"type": "TextMatch", "field": default_text_field_name_no_BM25, "text": "water"}])

        new_search_params = {"metric_type": "COSINE"}


        vector = client.query(collection_name, filter=f"{default_primary_key_field_name} == 2", output_fields=[default_vector_field_name])[0][default_vector_field_name]
        error = {ct.err_code: 1100,
                 ct.err_msg: f"Search highlight only support with metric type \"BM25\" but was: : invalid parameter"}
        self.search(
            client,
            collection_name,
            [vector],
            search_params=new_search_params,
            anns_field=default_vector_field_name,
            output_fields=[default_text_field_name_no_BM25], 
            limit=1,
            highlighter = highlight,
            check_task=CheckTasks.err_res,
            check_items=error
        )

    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_with_filter(self):
        """
        target: Test highlighter can be successfully used with query invalid search params
        method:
            1. Search the data with query invalid search params
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME

        highlight = LexicalHighlighter(pre_tags=["{"], post_tags=["}"], 
                        highlight_search_text = True, 
                        fragment_offset=0, 
                        fragment_size = 10,
                        num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        error = {ct.err_code: 1100,
                 ct.err_msg: f"failed to create query plan: cannot parse expression: TEXT_MATCH({default_text_field_name}, \"seat\"), "
                             f"error: field \"{default_text_field_name}\" does not enable match: invalid parameter"}
        
        self.search(
            client,
            collection_name, 
            ["water"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name], 
            highlighter = highlight,
            filter=f'TEXT_MATCH({default_text_field_name}, "seat")',
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_highlighter_empty_tags(self):
        """
        target: Test highlighter can be successfully used with filter invalid filter
        method:
            1. Search the data with filter invalid filter
        expected: Step 1 should result success
        """
        client = self._client()
        collection_name = COLLECTION_NAME
        highlight = LexicalHighlighter(pre_tags=[], post_tags=[],
                                       highlight_search_text = True, 
                                       fragment_offset=0, 
                                       fragment_size = 10,
                                       num_of_fragments=1)
        search_params = {"params": {"nlist": 128}, "metric_type": "BM25"}
        error = {ct.err_code: 1100,
                 ct.err_msg: f"pre_tags cannot be empty list: invalid parameter"}
        self.search(
            client,
            collection_name, 
            ["water"],
            search_params=search_params,
            anns_field=default_sparse_vector_field_name,
            output_fields=[default_text_field_name], 
            highlighter = highlight,
            check_task=CheckTasks.err_res,
            check_items=error
        )