from collections import Counter
import pandas as pd
import re
import time
 
def extract_hashtags_sequential(lines):
    hashtag_re = re.compile(r"#\w+")
    counter = Counter()
    for line in lines:
        hashtags = hashtag_re.findall(line.lower())
        counter.update(hashtags)
    return counter
 
if __name__ == "__main__":
    file_path = "/home/ec2-user/amazon_book_review_filename/Books_rating.csv"
    df = pd.read_csv(file_path, usecols=["review/text"])
    lines = df["review/text"].dropna().astype(str).tolist()
 
    start = time.time()
    counter = extract_hashtags_sequential(lines)
    end = time.time()
 
    print("✅ Sequential Hashtag Count Completed")
    print("Top 10 Hashtags:", counter.most_common(10))
    print("Unique Hashtags:", len(counter))
    print("Total Hashtag Mentions:", sum(counter.values()))
    print("Time Taken:", round(end - start, 2), "s")
 
    with open("hashtag_sequential_output.csv", "w") as f:
        f.write("hashtag,count\n")
        for tag, count in counter.items():
            f.write(f"{tag},{count}\n")
