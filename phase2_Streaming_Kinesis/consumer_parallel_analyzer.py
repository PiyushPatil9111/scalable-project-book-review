import boto3
import json
import time
from datetime import datetime, timedelta
from collections import Counter, deque
from multiprocessing import Pool, cpu_count, set_start_method
import matplotlib.pyplot as plt
import os
from textblob import TextBlob

# Set multiprocessing start method
try:
    set_start_method("fork")
except RuntimeError:
    pass

# AWS configuration
STREAM_NAME = "bookReviewStream"
REGION = "us-east-1"
S3_BUCKET = "scalable-youtube-comments-bucket-personal"

# AWS clients
kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3")

# Stopwords
stopwords = set("""
a an about above after again against all am an and any are aren't as at
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
you you'd you'll you're you've your yours yourself yourselves and are as at be by for from has he in is it its of on that the to was were will with you your i we our they this not
""".split())

# Sliding window state
word_window = deque()
sentiments = []
WINDOW_DURATION = timedelta(minutes=5)

def process_record(record_data):
    try:
        data = json.loads(record_data)
        text = data.get("text", "").lower()
        sent_at = datetime.fromisoformat(data.get("sent_at"))
        words = [w for w in text.split() if w.isalpha() and w not in stopwords and len(w) > 3]
        score = TextBlob(text).sentiment.polarity
        sentiment = "positive" if score > 0 else "negative" if score < 0 else "neutral"
        return words, sentiment
    except Exception:
        return None

if __name__ == "__main__":
    print(" Parallel consumer started (real-time filtering + sliding window)...")

    shard_id = kinesis.describe_stream(StreamName=STREAM_NAME)["StreamDescription"]["Shards"][0]["ShardId"]
    shard_iterator = kinesis.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="LATEST"
    )["ShardIterator"]

    pool = Pool(processes=cpu_count())
    minute_timer = time.time()

    while True:
        response = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
        shard_iterator = response["NextShardIterator"]
        now = datetime.now()

        records = [r["Data"] for r in response["Records"]]
        results = pool.map(process_record, records)

        for result in results:
            if result:
                words, sentiment = result
                word_window.append((now, words))
                sentiments.append(sentiment)

        # Remove records outside the 5-minute window
        cutoff = now - WINDOW_DURATION
        while word_window and word_window[0][0] < cutoff:
            word_window.popleft()

        # Every 60 seconds: analyze and report
        if time.time() - minute_timer >= 60:
            window_start = (now - WINDOW_DURATION).strftime('%Y-%m-%d %H:%M:%S')
            window_end = now.strftime('%Y-%m-%d %H:%M:%S')
            print("\n" + "*" * 60)
            print(f" Analysis Window: {window_start} to {window_end}")
            print(" Running real-time trending word + sentiment analysis...")

            # Top words
            counter = Counter()
            for _, words in word_window:
                counter.update(words)
            top_words = counter.most_common(5)

            if top_words:
                print(" Top 5 Trending Words:")
                for word, count in top_words:
                    print(f"  {word}: {count}")
                plt.figure()
                labels, counts = zip(*top_words)
                plt.bar(labels, counts, color="skyblue")
                plt.title("Top 5 Trending Words (last 5 mins)")
                plt.tight_layout()
                word_path = f"/tmp/words_parallel_{int(time.time())}.png"
                plt.savefig(word_path)
                plt.close()
                s3.upload_file(word_path, S3_BUCKET, f"charts/top_words/{os.path.basename(word_path)}")
                print(f" Word count chart uploaded to S3: charts/top_words/{os.path.basename(word_path)}")

            # Sentiment distribution
            sent_counter = Counter(sentiments)
            if sent_counter:
                print(" Sentiment Distribution:")
                for sent, count in sent_counter.items():
                    print(f"  {sent}: {count}")
                plt.figure()
                labels = list(sent_counter.keys())
                counts = list(sent_counter.values())
                plt.bar(labels, counts, color="orange")
                plt.title("Sentiment Distribution (last 5 mins)")
                plt.tight_layout()
                sent_path = f"/tmp/sentiment_parallel_{int(time.time())}.png"
                plt.savefig(sent_path)
                plt.close()
                s3.upload_file(sent_path, S3_BUCKET, f"charts/sentiment_dist/{os.path.basename(sent_path)}")
                print(f" Sentiment chart uploaded to S3: charts/sentiment_dist/{os.path.basename(sent_path)}")

            # Clear for next window
            sentiments.clear()
            minute_timer = time.time()
            print("*" * 60 + "\n")

        time.sleep(10)

