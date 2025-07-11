import pandas as pd
import time
from textblob import TextBlob
from collections import Counter
import matplotlib.pyplot as plt
import os
import boto3

# AWS S3 Setup
S3_BUCKET = "scalable-youtube-comments-bucket-personal"
s3 = boto3.client("s3")

# Paths
dataset_path = "/home/ec2-user/amazon_book_review_filename/Books_rating.csv"
plot_dir = "/tmp"

# Sizes (fractions)
data_fractions = [1/3, 2/3, 1.0]
labels = ["1/3 Dataset", "2/3 Dataset", "Full Dataset"]

metrics = []

for frac, label in zip(data_fractions, labels):
    print("******************************************")
    print(f"Running analysis for: {label}")
    print("Reading data...")

    df = pd.read_csv(dataset_path, usecols=["review/text"])
    df = df.dropna().sample(frac=frac, random_state=42).reset_index(drop=True)
    records = df["review/text"].astype(str).tolist()

    start_time = time.time()
    word_counter = Counter()
    sentiment_counter = Counter()
    latencies = []

    for text in records:
        t1 = time.time()
        words = [w.lower() for w in text.split() if w.isalpha() and len(w) > 3]
        word_counter.update(words)

        polarity = TextBlob(text).sentiment.polarity
        sentiment = "positive" if polarity > 0 else "negative" if polarity < 0 else "neutral"
        sentiment_counter[sentiment] += 1
        t2 = time.time()

        latencies.append(t2 - t1)

    end_time = time.time()
    total_time = end_time - start_time
    total_records = len(records)
    avg_latency = sum(latencies) / len(latencies)
    throughput = total_records / total_time

    print(f"Total Records: {total_records}")
    print(f"Avg Latency: {avg_latency:.4f} sec")
    print(f"Throughput: {throughput:.2f} records/sec")
    print(f"Time Taken to Process: {round(total_time, 2)} seconds")
    print("Top Words:", word_counter.most_common(5))
    print("Sentiment Distribution:", sentiment_counter)

    metrics.append({
        "label": label,
        "records": total_records,
        "latency": avg_latency,
        "throughput": throughput,
        "time": total_time
    })

    print("******************************************\n")

# Create comparison plots
metrics_df = pd.DataFrame(metrics)

# Plot throughput
plt.figure(figsize=(8, 5))
plt.bar(metrics_df["label"], metrics_df["throughput"], color="steelblue")
plt.title("Throughput vs Dataset Size")
plt.ylabel("Records per Second")
plt.xlabel("Dataset Size")
plt.tight_layout()
throughput_path = os.path.join(plot_dir, "throughput_comparison.png")
plt.savefig(throughput_path)
s3_key_tp = f"performance_plots/{os.path.basename(throughput_path)}"
s3.upload_file(throughput_path, S3_BUCKET, s3_key_tp)
print(f"Throughput comparison graph uploaded to S3 path: s3://{S3_BUCKET}/{s3_key_tp}")

# Plot latency
plt.figure(figsize=(8, 5))
plt.bar(metrics_df["label"], metrics_df["latency"], color="orange")
plt.title("Average Latency vs Dataset Size")
plt.ylabel("Latency (seconds)")
plt.xlabel("Dataset Size")
plt.tight_layout()
latency_path = os.path.join(plot_dir, "latency_comparison.png")
s3_key_lp = f"performance_plots/{os.path.basename(latency_path)}"
plt.savefig(latency_path)
s3.upload_file(latency_path, S3_BUCKET, s3_key_lp)
print(f"Latency comparison graph uploaded to S3 path: s3://{S3_BUCKET}/{s3_key_lp}")

# Plot processing time
plt.figure(figsize=(8, 5))
plt.plot(metrics_df["label"], metrics_df["time"], marker='o', color="green")
plt.title("Total Processing Time vs Dataset Size")
plt.ylabel("Time (seconds)")
plt.xlabel("Dataset Size")
plt.tight_layout()
time_path = os.path.join(plot_dir, "processing_time_comparison.png")
s3_key_pt = f"performance_plots/{os.path.basename(time_path)}"
plt.savefig(time_path)
s3.upload_file(time_path, S3_BUCKET, s3_key_pt)
print(f"Processing time comparison graph uploaded to S3 path: s3://{S3_BUCKET}/{s3_key_pt}")

print("All performance comparison plots have been uploaded to S3.")

