import boto3, json, time
from datetime import datetime, timedelta
from collections import Counter, deque
import matplotlib.pyplot as plt
import pandas as pd
from textblob import TextBlob
import os

# AWS Config
STREAM_NAME = "bookReviewStream"
REGION = "us-east-1"
S3_BUCKET = "scalable-youtube-comments-bucket-personal"

# AWS clients
kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3")

# Stopwords
stopwords = set("""a an and are as at be by for from has he in is it its of on that the to was were will with""".split())

# Stream iterator
shard_id = kinesis.describe_stream(StreamName=STREAM_NAME)["StreamDescription"]["Shards"][0]["ShardId"]
shard_iterator = kinesis.get_shard_iterator(StreamName=STREAM_NAME, ShardId=shard_id, ShardIteratorType="LATEST")["ShardIterator"]

# State
word_window = deque()
sentiments = []
latencies = []
metrics = []
WINDOW_DURATION = timedelta(minutes=5)

print("Sequential consumer started...")

minute_timer = time.time()
total_records = 0

while True:
    response = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
    shard_iterator = response["NextShardIterator"]
    now = datetime.now()

    for record in response["Records"]:
        data = json.loads(record["Data"])
        text = data.get("text", "").lower()
        sent_at = datetime.fromisoformat(data.get("sent_at"))
        latency = (datetime.utcnow() - sent_at).total_seconds()
        latencies.append(latency)
        total_records += 1

        words = [w for w in text.split() if w.isalpha() and w not in stopwords and len(w) > 3]
        word_window.append((now, words))

        score = TextBlob(text).sentiment.polarity
        sentiments.append("positive" if score > 0 else "negative" if score < 0 else "neutral")

    cutoff = now - WINDOW_DURATION
    while word_window and word_window[0][0] < cutoff:
        word_window.popleft()

    if time.time() - minute_timer >= 60:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Word count chart
        counter = Counter()
        for _, words in word_window:
            counter.update(words)
        top_words = counter.most_common(5)
        if top_words:
            labels, counts = zip(*top_words)
            plt.figure()
            plt.bar(labels, counts, color="blue")
            plt.title("Top 5 Words")
            plt.tight_layout()
            word_path = f"/tmp/words_{int(time.time())}.png"
            plt.savefig(word_path)
            plt.close()
            s3.upload_file(word_path, S3_BUCKET, f"charts/top_words/{os.path.basename(word_path)}")
            print(f"Word count analysis complete, uploaded to S3: charts/top_words/{os.path.basename(word_path)}")

        # Sentiment chart
        sent_counter = Counter(sentiments)
        if sent_counter:
            labels = list(sent_counter.keys())
            counts = list(sent_counter.values())
            plt.figure()
            plt.bar(labels, counts, color="green")
            plt.title("Sentiment Distribution")
            plt.tight_layout()
            sent_path = f"/tmp/sentiment_{int(time.time())}.png"
            plt.savefig(sent_path)
            plt.close()
            s3.upload_file(sent_path, S3_BUCKET, f"charts/sentiment_dist/{os.path.basename(sent_path)}")
            print(f"Sentiment analysis complete, uploaded to S3: charts/sentiment_dist/{os.path.basename(sent_path)}")

        # Metrics
        avg_latency = round(sum(latencies)/len(latencies), 2) if latencies else 0
        metrics.append({
            "timestamp": timestamp,
            "throughput": total_records,
            "avg_latency": avg_latency
        })
        csv_path = "/tmp/stream_metrics_seq.csv"
        pd.DataFrame(metrics).to_csv(csv_path, index=False)
        csv_key = f"metrics/stream_metrics_seq_{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.csv"
        s3.upload_file(csv_path, S3_BUCKET, csv_key)
        print(f"Metrics CSV uploaded to S3: {csv_key}")
        print(f"[{timestamp}] → Records: {total_records}, Avg Latency: {avg_latency}s")

        # Reset
        total_records = 0
        latencies.clear()
        sentiments.clear()
        minute_timer = time.time()

    time.sleep(10)

