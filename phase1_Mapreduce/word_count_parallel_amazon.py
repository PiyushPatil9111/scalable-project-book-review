from multiprocessing import Pool, cpu_count, set_start_method
from collections import Counter
import pandas as pd
import time
import string

try:
    set_start_method("fork")
except RuntimeError:
    pass

# Define stopwords
stopwords = set("""
a about above after again against all am an and any are aren't as at
be because been before being below between both but by
can can't cannot could couldn't
did didn't do does doesn't doing don't down during
each few for from further
had hadn't has hasn't have haven't having he he'd he'll he's her here here's hers herself him himself his how how's
i i'd i'll i'm i've if in into is isn't it it's its itself
let's
me more most mustn't my myself
no nor not now
of off on once only or other ought our ours ourselves out over own
same shan't she she'd she'll she's should shouldn't so some such
than that that's the their theirs them themselves then there there's these they they'd they'll they're they've this those through to too
under until up
very
was wasn't we we'd we'll we're we've were weren't what what's when when's where where's which while who who's whom why why's with won't would wouldn't
you you'd you'll you're you've your yours yourself yourselves  an and are as at be by for from has he in is it its of on or that the this to was were will with you your they them we i my me our us not
""".split())

# Count filtered words in a chunk
def count_words_chunk(chunk):
    counter = Counter()
    for line in chunk:
        words = line.strip().lower().split()
        for word in words:
            word = word.strip(string.punctuation)
            if word.isalpha() and word not in stopwords and len(word) > 3:
                counter[word] += 1
    return counter

# Split data into equal chunks
def split_data(data, num_chunks):
    chunk_size = len(data) // num_chunks
    return [data[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]

if __name__ == "__main__":
    file_path = "/home/ec2-user/amazon_book_review_filename/Books_rating.csv"
    print("Reading file...")
    df = pd.read_csv(file_path, usecols=["review/text"])
    lines = df["review/text"].dropna().astype(str).tolist()

    num_cores = cpu_count()
    chunks = split_data(lines, num_cores)

    print(f" Using {num_cores} cores for parallel processing...")
    start = time.time()
    with Pool(processes=num_cores) as pool:
        counters = pool.map(count_words_chunk, chunks)

    total_counter = sum(counters, Counter())
    end = time.time()

    print("Parallel Word Count Completed")
    print("Top 10 Words:", total_counter.most_common(10))
    print("Unique Words:", len(total_counter))
    print("Total Words:", sum(total_counter.values()))
    print("Time Taken:", round(end - start, 2), "s")

    with open("wordcount_parallel_output.csv", "w") as f:
        f.write("word,count\n")
        for word, count in total_counter.items():
            f.write(f"{word},{count}\n")

