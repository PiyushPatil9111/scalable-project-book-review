from collections import Counter
import pandas as pd
import time
 
def count_words_sequential(lines):
    counter = Counter()
    for line in lines:
        counter.update(line.strip().lower().split())
    return counter
 
if __name__ == "__main__":
    file_path = "/home/ec2-user/amazon_book_review_filename/Books_rating.csv"
    print("📥 Reading file...")
    df = pd.read_csv(file_path, usecols=["review/text"])
    lines = df["review/text"].dropna().astype(str).tolist()
 
    start = time.time()
    word_counter = count_words_sequential(lines)
    end = time.time()
 
    print("✅ Sequential Word Count Completed")
    print("Top 10 Words:", word_counter.most_common(10))
    print("Unique Words:", len(word_counter))
    print("Total Words:", sum(word_counter.values()))
    print("Time Taken:", round(end - start, 2), "s")
 
    with open("wordcount_sequential_output.csv", "w") as f:
        f.write("word,count\n")
        for word, count in word_counter.items():
            f.write(f"{word},{count}\n")
