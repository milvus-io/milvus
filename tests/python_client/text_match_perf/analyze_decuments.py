from collections import Counter
import time
import bm25s
import jieba
import pandas as pd
import glob
import polars as pl
def analyze_documents(texts, language="en"):
    # Create a stemmer
    # stemmer = Stemmer.Stemmer("english")
    stopwords = "en"
    if language in ["en", "english"]:
        stopwords= "en"
    if language in ["zh", "cn", "chinese"]:
        stopword = " "
        new_texts = []
        for doc in texts:
            seg_list = jieba.cut(doc)
            new_texts.append(" ".join(seg_list))
        texts = new_texts
        stopwords = [stopword]
    # Start timing
    t0 = time.time()

    # Tokenize the corpus
    tokenized = bm25s.tokenize(texts, lower=True, stopwords=stopwords)
    # log.info(f"Tokenized: {tokenized}")
    # Create a frequency counter
    freq = Counter()

    # Count the frequency of each token
    for doc_ids in tokenized.ids:
        freq.update(doc_ids)
    # Create a reverse vocabulary mapping
    id_to_word = {id: word for word, id in tokenized.vocab.items()}

    # Convert token ids back to words
    word_freq = Counter({id_to_word[token_id]: count for token_id, count in freq.items()})

    # End timing
    tt = time.time() - t0
    print(f"Analyze document cost time: {tt}")

    return word_freq, tokenized

if __name__ == "__main__":
    files = glob.glob("./train-*.parquet")
    df = pl.read_parquet("./train-*.parquet")
    word_word_freq, word_tokenized = analyze_documents(df["word"].to_list())
    sentence_word_freq, sentence_tokenized = analyze_documents(df["sentence"].to_list())
    paragraph_word_freq, paragraph_tokenized = analyze_documents(df["paragraph"].to_list())
    text_word_freq, text_tokenized = analyze_documents(df["text"].to_list())
    print(word_word_freq)
    print(sentence_word_freq)

