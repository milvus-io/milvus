from faker import Faker
import random

class ICUTextGenerator:
    """
    ICU(International Components for Unicode)TextGenerator: 
    Generate test sentences containing multiple languages (Chinese, English, Japanese, Korean), emojis, and special symbols.
    """
    def __init__(self):
        self.fake_en = Faker("en_US")
        self.fake_zh = Faker("zh_CN")
        self.fake_ja = Faker("ja_JP")
        self.fake_de = Faker("de_DE")
        self.korean_samples = [
            "안녕하세요 세계", "파이썬 프로그래밍", "데이터 분석", "인공지능",
            "밀버스 테스트", "한국어 샘플", "자연어 처리"
        ]
        self.emojis = ["😊", "🐍", "🚀", "🌏", "💡", "🔥", "✨", "👍"]
        self.specials = ["#", "@", "$"]

    def word(self):
        """
        Generate a list of words containing multiple languages, emojis, and special symbols.
        """
        parts = [
            self.fake_en.word(),
            self.fake_zh.word(),
            self.fake_ja.word(),
            self.fake_de.word(),
            random.choice(self.korean_samples),
            random.choice(self.emojis),
            random.choice(self.specials),
        ]
        return  random.choice(parts)

    def sentence(self):
        """
        Generate a sentence containing multiple languages, emojis, and special symbols.
        """
        parts = [
            self.fake_en.sentence(),
            self.fake_zh.sentence(),
            self.fake_ja.sentence(),
            self.fake_de.sentence(),
            random.choice(self.korean_samples),
            " ".join(random.sample(self.emojis, 2)),
            " ".join(random.sample(self.specials, 2)),
        ]
        random.shuffle(parts)
        return " ".join(parts)

    def paragraph(self, num_sentences=3):
        """
        Generate a paragraph containing multiple sentences, each with multiple languages, emojis, and special symbols.
        """
        return ' '.join([self.sentence() for _ in range(num_sentences)])

    def text(self, num_sentences=5):
        """
        Generate multiple sentences containing multiple languages, emojis, and special symbols.
        """
        return ' '.join([self.sentence() for _ in range(num_sentences)])


class KoreanTextGenerator:
    """
    KoreanTextGenerator: Generate test sentences containing Korean activities, verbs, connectors, and modifiers.
    """
    def __init__(self):
        # Sports/Activities (Nouns)
        self.activities = [
            "수영", "축구", "농구", "테니스",
            "배구", "야구", "골프", "럭비",
            "달리기", "자전거", "스케이트", "스키",
            "서핑", "다이빙", "등산", "요가",
            "춤", "하이킹", "독서", "요리"
        ]

        # Verbs (Base Form)
        self.verbs = [
            "좋아하다", "즐기다", "하다", "배우다",
            "가르치다", "보다", "시작하다", "계속하다",
            "연습하다", "선호하다", "마스터하다", "도전하다"
        ]

        # Connectors
        self.connectors = [
            "그리고", "또는", "하지만", "그런데",
            "그래서", "또한", "게다가", "그러면서",
            "동시에", "함께"
        ]

        # Modifiers (Frequency/Degree)
        self.modifiers = [
            "매우", "자주", "가끔", "열심히",
            "전문적으로", "규칙적으로", "매일", "일주일에 한 번",
            "취미로", "진지하게"
        ]

    def conjugate_verb(self, verb):
        # Simple Korean verb conjugation (using informal style "-아/어요")
        if verb.endswith("하다"):
            return verb.replace("하다", "해요")
        elif verb.endswith("다"):
            return verb[:-1] + "아요"
        return verb


    def word(self):
        return random.choice(self.activities + self.verbs + self.modifiers + self.connectors)

    def sentence(self):
        # Build basic sentence structure
        activity = random.choice(self.activities)
        verb = random.choice(self.verbs)
        modifier = random.choice(self.modifiers)

        # Conjugate verb
        conjugated_verb = self.conjugate_verb(verb)

        # Build sentence (Korean word order: Subject + Object + Modifier + Verb)
        sentence = f"저는 {activity}를/을 {modifier} {conjugated_verb}"

        # Randomly add connector and another activity
        if random.choice([True, False]):
            connector = random.choice(self.connectors)
            second_activity = random.choice(self.activities)
            second_verb = self.conjugate_verb(random.choice(self.verbs))
            sentence += f" {connector} {second_activity}도 {second_verb}"

        return sentence + "."

    def paragraph(self, num_sentences=3):
        return '\n'.join([self.sentence() for _ in range(num_sentences)])

    def text(self, num_sentences=5):
        return '\n'.join([self.sentence() for _ in range(num_sentences)])


def generate_text_by_analyzer(analyzer_params):
    """
    Generate text data based on the given analyzer parameters

    Args:
        analyzer_params: Dictionary containing the analyzer parameters

    Returns:
        str: Generated text data
    """
    if analyzer_params["tokenizer"] == "standard":
        fake = Faker("en_US")
    elif analyzer_params["tokenizer"] == "jieba":
        fake = Faker("zh_CN")
    elif analyzer_params["tokenizer"] == "icu":
        fake = ICUTextGenerator()

    elif analyzer_params["tokenizer"]["type"] == "lindera":
        # Generate random Japanese text
        if analyzer_params["tokenizer"]["dict_kind"] == "ipadic":
            fake = Faker("ja_JP")
        elif analyzer_params["tokenizer"]["dict_kind"] == "ko-dic":
            fake = KoreanTextGenerator()
        elif analyzer_params["tokenizer"]["dict_kind"] == "cc-cedict":
            fake = Faker("zh_CN")
        else:
            raise ValueError("Invalid dict_kind")
    else:
        raise ValueError("Invalid analyzer parameters")

    text = fake.text()
    stop_words = []
    if "filter" in analyzer_params:
        for filter in analyzer_params["filter"]:
            if filter["type"] == "stop":
                stop_words.extend(filter["stop_words"])

    # add stop words to the text
    text += " " + " ".join(stop_words)
    return text
