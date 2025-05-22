import re
import jieba
from faker import Faker
from tantivy import SchemaBuilder, Document, Index, Query
from typing import List, Dict
import numpy as np
import random


class PhraseMatchTestGenerator:
    def __init__(self, language="en"):
        """
        Initialize the test data generator

        Args:
            language: Language for text generation ('en' for English, 'zh' for Chinese)
        """
        self.language = language
        self.index = None
        self.documents = []

        # English vocabulary
        self.en_activities = [
            "swimming",
            "football",
            "basketball",
            "tennis",
            "volleyball",
            "baseball",
            "golf",
            "rugby",
            "cricket",
            "boxing",
            "running",
            "cycling",
            "skating",
            "skiing",
            "surfing",
            "diving",
            "climbing",
            "yoga",
            "dancing",
            "hiking",
        ]

        self.en_verbs = [
            "love",
            "like",
            "enjoy",
            "play",
            "practice",
            "prefer",
            "do",
            "learn",
            "teach",
            "watch",
            "start",
            "begin",
            "continue",
            "finish",
            "master",
            "try",
        ]

        self.en_connectors = [
            "and",
            "or",
            "but",
            "while",
            "after",
            "before",
            "then",
            "also",
            "plus",
            "with",
        ]

        self.en_modifiers = [
            "very much",
            "a lot",
            "seriously",
            "casually",
            "professionally",
            "regularly",
            "often",
            "sometimes",
            "daily",
            "weekly",
        ]

        # Chinese vocabulary
        self.zh_activities = [
            "游泳",
            "足球",
            "篮球",
            "网球",
            "排球",
            "棒球",
            "高尔夫",
            "橄榄球",
            "板球",
            "拳击",
            "跑步",
            "骑行",
            "滑冰",
            "滑雪",
            "冲浪",
            "潜水",
            "攀岩",
            "瑜伽",
            "跳舞",
            "徒步",
        ]

        self.zh_verbs = [
            "喜欢",
            "热爱",
            "享受",
            "玩",
            "练习",
            "偏好",
            "做",
            "学习",
            "教",
            "观看",
            "开始",
            "开启",
            "继续",
            "完成",
            "掌握",
            "尝试",
        ]

        self.zh_connectors = [
            "和",
            "或者",
            "但是",
            "同时",
            "之后",
            "之前",
            "然后",
            "也",
            "加上",
            "跟",
        ]

        self.zh_modifiers = [
            "非常",
            "很多",
            "认真地",
            "随意地",
            "专业地",
            "定期地",
            "经常",
            "有时候",
            "每天",
            "每周",
        ]

        # Set vocabulary based on language
        self.activities = self.zh_activities if language == "zh" else self.en_activities
        self.verbs = self.zh_verbs if language == "zh" else self.en_verbs
        self.connectors = self.zh_connectors if language == "zh" else self.en_connectors
        self.modifiers = self.zh_modifiers if language == "zh" else self.en_modifiers

    def tokenize_text(self, text: str) -> List[str]:
        """Tokenize text using jieba tokenizer"""
        text = text.strip()
        text = re.sub(r"[^\w\s]", " ", text)
        text = text.replace("\n", " ")
        if self.language == "zh":
            text = text.replace(" ", "")
            return list(jieba.cut_for_search(text))
        else:
            return list(text.split())

    def generate_embedding(self, dim: int) -> List[float]:
        """Generate random embedding vector"""
        return list(np.random.random(dim))

    def generate_text_pattern(self) -> str:
        """Generate test document text with various patterns"""
        patterns = [
            # Simple pattern with two activities
            lambda: f"{random.choice(self.activities)} {random.choice(self.activities)}",
            # Pattern with connector between activities
            lambda: f"{random.choice(self.activities)} {random.choice(self.connectors)} {random.choice(self.activities)}",
            # Pattern with modifier between activities
            lambda: f"{random.choice(self.activities)} {random.choice(self.modifiers)} {random.choice(self.activities)}",
            # Complex pattern with verb and activities
            lambda: f"{random.choice(self.verbs)} {random.choice(self.activities)} {random.choice(self.activities)}",
            # Pattern with multiple gaps
            lambda: f"{random.choice(self.activities)} {random.choice(self.modifiers)} {random.choice(self.connectors)} {random.choice(self.activities)}",
        ]
        return random.choice(patterns)()

    def generate_test_data(self, num_documents: int, dim: int) -> List[Dict]:
        """
        Generate test documents with text and embeddings

        Args:
            num_documents: Number of documents to generate
            dim: Dimension of embedding vectors

        Returns:
            List of dictionaries containing document data
        """
        # Generate documents
        self.documents = []
        for i in range(num_documents):
            self.documents.append(
                {
                    "id": i,
                    "text": self.generate_text_pattern()
                    if self.language == "en"
                    else self.generate_text_pattern().replace(" ", ""),
                    "emb": self.generate_embedding(dim),
                }
            )

        # Initialize Tantivy index
        schema_builder = SchemaBuilder()

        schema_builder.add_text_field("text", stored=True)
        schema_builder.add_unsigned_field("doc_id", stored=True)
        schema = schema_builder.build()

        self.index = Index(schema=schema, path=None)

        writer = self.index.writer()

        # Index all documents
        for doc in self.documents:
            document = Document()
            new_text = " ".join(self.tokenize_text(doc["text"]))
            document.add_text("text", new_text)
            document.add_unsigned("doc_id", doc["id"])
            writer.add_document(document)

        writer.commit()
        self.index.reload()

        return self.documents

    def _generate_random_word(self, exclude_words: List[str]) -> str:
        """
        Generate a random word that is not in the exclude_words list using Faker
        """
        fake = Faker()
        while True:
            word = fake.word()
            if word not in exclude_words:
                return word

    def generate_pattern_documents(self, patterns: List[tuple], dim: int, num_docs_per_pattern: int = 1) -> List[Dict]:
        """
        Generate documents that match specific test patterns with their corresponding slop values

        Args:
            patterns: List of tuples containing (pattern, slop) pairs
            dim: Dimension of embedding vectors
            num_docs_per_pattern: Number of documents to generate for each pattern

        Returns:
            List of dictionaries containing document data with text and embeddings
        """
        pattern_documents = []
        for pattern, slop in patterns:
            # Split pattern into components
            pattern_words = pattern.split()

            # Generate multiple documents for each pattern
            if slop == 0:  # Exact phrase
                text = " ".join(pattern_words)
                pattern_documents.append({
                    "id": random.randint(0, 1000000), "text": text, "emb": self.generate_embedding(dim)})

            else:  # Pattern with gaps
                # Generate slop number of unique words
                insert_words = []
                for _ in range(slop):
                    new_word = self._generate_random_word(pattern_words + insert_words)
                    insert_words.append(new_word)

                # Insert the words randomly between the pattern words
                all_words = pattern_words.copy()
                for word in insert_words:
                    # Random position between pattern words
                    pos = random.randint(1, len(all_words))
                    all_words.insert(pos, word)

                text = " ".join(all_words)
                pattern_documents.append({
                    "id": random.randint(0, 1000000),
                    "text": text,
                    "emb": self.generate_embedding(dim)})

        new_pattern_documents = []
        start = 1000000
        for i in range(num_docs_per_pattern):
            for doc in pattern_documents:
                new_doc = dict(doc)
                new_doc["id"] = start + len(new_pattern_documents)
                new_pattern_documents.append(new_doc)

        return new_pattern_documents

    def generate_test_queries(self, num_queries: int) -> List[Dict]:
        """
        Generate test queries with varying slop values

        Args:
            num_queries: Number of queries to generate

        Returns:
            List of dictionaries containing query information
        """
        queries = []
        slop_values = [0, 1, 2, 3]  # Common slop values

        for i in range(num_queries):
            # Randomly select two or three words for the query
            num_words = random.choice([2, 3])
            words = random.sample(self.activities, num_words)

            queries.append(
                {
                    "id": i,
                    "query": " ".join(words)
                    if self.language == "en"
                    else "".join(words),
                    "slop": random.choice(slop_values),
                    "type": f"{num_words}_words",
                }
            )

        return queries

    def get_query_results(self, query: str, slop: int) -> List[Dict]:
        """
        Get all documents that match the phrase query

        Args:
            query: Query phrase
            slop: Maximum allowed word gap

        Returns:
            List[Dict]: List of matching documents with their ids and texts
        """
        if self.index is None:
            raise RuntimeError("No documents indexed. Call generate_test_data first.")

        # Clean and normalize query
        query_terms = self.tokenize_text(query)

        # Create phrase query
        searcher = self.index.searcher()
        phrase_query = Query.phrase_query(self.index.schema, "text", query_terms, slop)

        # Search for matches
        results = searcher.search(phrase_query, limit=len(self.documents))

        # Extract all matching documents
        matched_docs = []
        for _, doc_address in results.hits:
            doc = searcher.doc(doc_address)
            doc_id = doc.to_dict()["doc_id"]
            matched_docs.extend(doc_id)

        return matched_docs

