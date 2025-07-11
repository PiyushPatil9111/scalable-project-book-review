import boto3
import threading
import time

# Configuration
AMI_ID = 'ami-0d84a0d24b77f81ae'  # Amazon Linux 2 AMI (update if needed)
INSTANCE_TYPE = 't2.micro'
KEY_NAME = 'scalable-project-instance'       
REGION = 'us-east-1'
S3_BUCKET = 'scalable-youtube-comments-bucket-personal'
INPUT_FILE = 'Books_rating.csv'
TASKS = ['wordcount', 'sentiment', 'hashtag']

def upload_scripts_to_s3():
    """Upload all task scripts to S3 bucket"""
    s3 = boto3.client('s3', region_name=REGION)
    
    scripts = {
        'wordcount.py': """
from multiprocessing import Pool, cpu_count, set_start_method
from collections import Counter
import pandas as pd
import string
import time

try:
    set_start_method("fork")
except RuntimeError:
    pass

# Define stopwords
stopwords = set(\"\"\"
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
\"\"\".split())

def count_words_chunk(chunk):
    counter = Counter()
    for line in chunk:
        words = line.strip().lower().split()
        for word in words:
            word = word.strip(string.punctuation)
            if word.isalpha() and word not in stopwords and len(word) > 3:
                counter[word] += 1
    return counter

def split_data(data, num_chunks):
    chunk_size = len(data) // num_chunks
    return [data[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]

def main():
    print("Reading input file...")
    df = pd.read_csv("input.csv", usecols=["review/text"])
    lines = df["review/text"].dropna().astype(str).tolist()

    num_cores = cpu_count()
    chunks = split_data(lines, num_cores)

    print(f"Using {num_cores} cores for parallel processing...")
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

    with open("wordcount_output.csv", "w") as f:
        f.write("word,count\\n")
        for word, count in total_counter.items():
            f.write(f"{word},{count}\\n")

if __name__ == "__main__":
    main()
""",
        
        'sentiment.py': """
from multiprocessing import Pool, cpu_count, set_start_method
from collections import Counter
from textblob import TextBlob
import pandas as pd
import time

try:
    set_start_method("fork")
except RuntimeError:
    pass

def process_text(text):
    words = [w.lower() for w in str(text).split() if w.isalpha() and len(w) > 3]
    polarity = TextBlob(text).sentiment.polarity
    sentiment = "positive" if polarity > 0 else "negative" if polarity < 0 else "neutral"
    return words, sentiment

def analyze_sentiment_chunk(chunk):
    sentiment_counter = Counter()
    for text in chunk:
        _, sentiment = process_text(text)
        sentiment_counter[sentiment] += 1
    return sentiment_counter

def split_data(data, num_chunks):
    chunk_size = len(data) // num_chunks
    return [data[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]

def main():
    print("Reading input file...")
    df = pd.read_csv("input.csv", usecols=["review/text"])
    lines = df["review/text"].dropna().astype(str).tolist()

    num_cores = cpu_count()
    chunks = split_data(lines, num_cores)

    print(f"Using {num_cores} cores for parallel processing...")
    start = time.time()
    with Pool(processes=num_cores) as pool:
        counters = pool.map(analyze_sentiment_chunk, chunks)

    total_counter = sum(counters, Counter())
    end = time.time()

    print("Sentiment Analysis Completed")
    print("Sentiment Distribution:", total_counter.most_common())
    print("Time Taken:", round(end - start, 2), "s")

    with open("sentiment_output.csv", "w") as f:
        f.write("sentiment,count\\n")
        for sentiment, count in total_counter.items():
            f.write(f"{sentiment},{count}\\n")

if __name__ == "__main__":
    main()
""",
        
        'hashtag.py': """
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
    hashtag_re = re.compile(r"#\\w+")
    counter = Counter()
    for line in chunk:
        hashtags = hashtag_re.findall(line.lower())
        counter.update(hashtags)
    return counter

def split_data(data, num_chunks):
    chunk_size = len(data) // num_chunks
    return [data[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]

def main():
    print("Reading input file...")
    df = pd.read_csv("input.csv", usecols=["review/text"])
    lines = df["review/text"].dropna().astype(str).tolist()

    num_cores = cpu_count()
    chunks = split_data(lines, num_cores)

    print(f"Using {num_cores} cores for parallel processing...")
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

    with open("hashtag_output.csv", "w") as f:
        f.write("hashtag,count\\n")
        for tag, count in total_counter.items():
            f.write(f"{tag},{count}\\n")

if __name__ == "__main__":
    main()
"""
    }
    
    for filename, content in scripts.items():
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f'scripts/{filename}',
            Body=content
        )
        print(f"Uploaded {filename} to S3")

def launch_task(task_type):
    ec2 = boto3.resource('ec2', region_name=REGION)
    print(f"Launching EC2 for task: {task_type}")
    
    user_data_script = f"""#!/bin/bash
# Install dependencies
sudo yum update -y
sudo yum install -y python3 python3-pip
sudo pip3 install pandas textblob boto3

# Configure AWS CLI (region only)
mkdir -p /home/ec2-user/.aws
echo "[default]\\nregion = {REGION}" > /home/ec2-user/.aws/config

# Download scripts and data from S3
aws s3 cp s3://{S3_BUCKET}/scripts/{task_type}.py /home/ec2-user/{task_type}.py
aws s3 cp s3://{S3_BUCKET}/{INPUT_FILE} /home/ec2-user/input.csv

# Run the analysis
cd /home/ec2-user
python3 {task_type}.py

# Upload results back to S3
aws s3 cp /home/ec2-user/{task_type}_output.csv s3://{S3_BUCKET}/results/

# Shutdown when done
sudo shutdown -h now
"""
    
    instances = ec2.create_instances(
        ImageId=AMI_ID,
        InstanceType=INSTANCE_TYPE,
        MinCount=1,
        MaxCount=1,
        KeyName=KEY_NAME,
        UserData=user_data_script,
        IamInstanceProfile={
            'Name': 'scalable-project-instance-role'
        },
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{'Key': 'Task', 'Value': task_type}]
        }]
    )
    
    instance = instances[0]
    print(f"Launched instance {instance.id} for task {task_type}")
    return instance.id

if __name__ == "__main__":
    print("=== Starting Parallel Analysis Tasks ===")
    
    # 1. Upload all scripts to S3
    print("\nUploading task scripts to S3...")
    upload_scripts_to_s3()
    
    # 2. Launch EC2 instances for each task
    print("\nLaunching EC2 instances for each task...")
    threads = []
    
    for task in TASKS:
        t = threading.Thread(target=launch_task, args=(task,))
        threads.append(t)
        t.start()
        time.sleep(2)  # Small delay between launches
    
    for t in threads:
        t.join()
    
    print("\n=== All tasks launched successfully ===")
    print("Results will be available in:")
    print(f"s3://{S3_BUCKET}/results/")
    print("\nMonitor EC2 instances in AWS Console for completion status.")
