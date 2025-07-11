import boto3
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime

# AWS Configuration
S3_BUCKET = "scalable-youtube-comments-bucket-personal"
CSV_PREFIX = "metrics/"
GRAPH_UPLOAD_PREFIX = "performance-graphs/"

# Initialize boto3 S3 client
s3 = boto3.client("s3")

# Step 1: Find the latest metrics CSV file
def get_latest_metrics_csv():
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=CSV_PREFIX)
    if "Contents" not in response:
        print("No files found in S3 under 'metrics/' prefix.")
        return None

    csv_files = [
        obj["Key"] for obj in response["Contents"]
        if obj["Key"].endswith(".csv")
    ]
    
    if not csv_files:
        print("No CSV files found in S3.")
        return None
    
    # Sort by key name (timestamp is embedded)
    csv_files.sort(reverse=True)
    return csv_files[0]

# Step 2: Download CSV from S3
def download_csv(csv_key):
    response = s3.get_object(Bucket=S3_BUCKET, Key=csv_key)
    data = response["Body"].read()
    return pd.read_csv(BytesIO(data))

# Step 3: Generate and save plots
def generate_and_upload_plots(df):
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    timestamp_str = datetime.now().strftime("%Y%m%d-%H%M%S")

    # Plot 1: Throughput
    plt.figure(figsize=(10, 5))
    plt.plot(df["timestamp"], df["throughput"], marker='o', color='blue')
    plt.title("Throughput Over Time")
    plt.xlabel("Time")
    plt.ylabel("Records per Minute")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    throughput_plot_file = f"/tmp/throughput_plot_{timestamp_str}.png"
    plt.savefig(throughput_plot_file)
    plt.close()

    # Plot 2: Latency
    plt.figure(figsize=(10, 5))
    plt.plot(df["timestamp"], df["avg_latency"], marker='s', color='green')
    plt.title("Average Latency Over Time")
    plt.xlabel("Time")
    plt.ylabel("Latency (seconds)")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    latency_plot_file = f"/tmp/latency_plot_{timestamp_str}.png"
    plt.savefig(latency_plot_file)
    plt.close()

    # Upload plots to S3
    s3.upload_file(throughput_plot_file, S3_BUCKET, f"{GRAPH_UPLOAD_PREFIX}throughput_plot_{timestamp_str}.png")
    s3.upload_file(latency_plot_file, S3_BUCKET, f"{GRAPH_UPLOAD_PREFIX}latency_plot_{timestamp_str}.png")

    print(f"Plots uploaded to S3 in folder '{GRAPH_UPLOAD_PREFIX}'.")

# Main execution
if __name__ == "__main__":
    latest_csv_key = get_latest_metrics_csv()
    if not latest_csv_key:
        exit(1)

    print(f"Downloading CSV: {latest_csv_key}")
    df = download_csv(latest_csv_key)

    print("Generating and uploading plots...")
    generate_and_upload_plots(df)

