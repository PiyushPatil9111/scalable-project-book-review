import boto3
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime

# Configuration
BUCKET = "scalable-youtube-comments-bucket-personal"
PREFIX = "metrics/"
GRAPH_PREFIX = "performance-graphs/"
REGION = "us-east-1"

s3 = boto3.client("s3", region_name=REGION)

def list_csv_files():
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    if "Contents" not in response:
        return [], []
    files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".csv")]
    seq_files = sorted([f for f in files if "seq" in f])
    par_files = sorted([f for f in files if "parallel" in f])
    return seq_files, par_files

def download_csv(key):
    response = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(BytesIO(response['Body'].read()))

def combine_metrics(files):
    frames = []
    for f in files:
        df = download_csv(f)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        frames.append(df)
    return pd.concat(frames).sort_values("timestamp")

def plot_comparison(df_seq, df_par, metric, ylabel, filename):
    plt.figure(figsize=(10, 6))
    plt.plot(df_seq["timestamp"], df_seq[metric], label="Sequential", color="blue", marker='o')
    plt.plot(df_par["timestamp"], df_par[metric], label="Parallel", color="green", marker='x')
    plt.title(f"{ylabel} Comparison (Sequential vs Parallel)")
    plt.xlabel("Timestamp")
    plt.ylabel(ylabel)
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    local_path = f"/tmp/{filename}"
    plt.savefig(local_path)
    plt.close()
    s3.upload_file(local_path, BUCKET, f"{GRAPH_PREFIX}{filename}")
    print(f"Uploaded {filename} to S3")

if __name__ == "__main__":
    seq_keys, par_keys = list_csv_files()
    if not seq_keys or not par_keys:
        print("Missing sequential or parallel CSVs.")
        exit(1)

    df_seq = combine_metrics(seq_keys)
    df_par = combine_metrics(par_keys)

    plot_comparison(df_seq, df_par, "throughput", "Throughput (records/min)", "throughput_comparison.png")
    plot_comparison(df_seq, df_par, "avg_latency", "Latency (seconds)", "latency_comparison.png")

