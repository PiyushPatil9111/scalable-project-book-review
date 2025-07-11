from multiprocessing import Pool, cpu_count, set_start_method
from collections import Counter
import pandas as pd
import re
import time
 
try:
    set_start_method("fork")
except RuntimeError:
    pass
 
def extract_hashtags_chunk(chunk):
    hashtag_re = re.compile(r"#\w+")
    counter = Counter()
    for line in chunk:
        hashtags = hashtag_re.findall(line.lower())
        counter.update(hashtags)
    return counter
 
def split_data(data, num_chunks):
    chunk_size = len(data) // num_chunks
    return [data[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]
 
if __name__ == "__main__":
    file_path = "/home/ec2-user/amazon_book_review_filename/Books_rating.csv"
    df = pd.read_csv(file_path, usecols=["review/text"])
    lines = df["review/text"].dropna().astype(str).tolist()
 
    num_cores = cpu_count()
    chunks = split_data(lines, num_cores)
 
    start = time.time()
    with Pool(processes=num_cores) as pool:
        counters = pool.map(extract_hashtags_chunk, chunks)
 
    total_counter = sum(counters, Counter())
    end = time.time()
 
    print("✅ Parallel Hashtag Count Completed")
    print("Top 10 Hashtags:", total_counter.most_common(10))
    print("Unique Hashtags:", len(total_counter))
    print("Total Hashtag Mentions:", sum(total_counter.values()))
    print("Time Taken:", round(end - start, 2), "s")
 
    with open("hashtag_parallel_output.csv", "w") as f:
        f.write("hashtag,count\n")
        for tag, count in total_counter.items():
            f.write(f"{tag},{count}\n")
