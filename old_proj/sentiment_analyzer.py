import json
import boto3
from textblob import TextBlob
from pyspark.sql import SparkSession

bucket_name = "scalable-youtube-comments-bucket"


def extract_comments(json_obj):
    comments = [] 
    for item in json_obj.get("items", []):
        text = item['snippet']['topLevelComment']['snippet']['textDisplay']
        comments.append(text)
    return comments

def analyze_sentiment(comments):
    results = []
    for text in comments:
        score = TextBlob(text).sentiment.polarity
        results.append((text, score))
    return results

def main():

    spark = SparkSession.builder \
    .appName("YouTubeCommentSentiment") \
    .config("spark.jars",
        "/home/ec2-user/spark_clean_jars/hadoop-aws-3.3.4.jar,"
        "/home/ec2-user/spark_clean_jars/aws-java-sdk-core-1.12.99.jar,"
        "/home/ec2-user/spark_clean_jars/aws-java-sdk-s3-1.12.99.jar,"
        "/home/ec2-user/spark_clean_jars/aws-java-sdk-dynamodb-1.12.99.jar") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.conf.dir", "/home/ec2-user/hadoop_conf") \
    .config("spark.hadoop.fs.s3a.access.key", "ASIAYLK5FF6ZRZSKK7ZM") \
    .config("spark.hadoop.fs.s3a.secret.key", "WujQcP4EZpHzuJAH2s1BxNT4oRRtWf14STPNFsIL") \
    .config("spark.hadoop.fs.s3a.session.token", "IQoJb3JpZ2luX2VjEFsaCXVzLXdlc3QtMiJIMEYCIQCbScKiGwqr46Zz+B62YbFUembFlje1WSyWXx/2PMlzxwIhAOeJ8k2sCoHhPcxjeTg/6+CaySXKTcOr70+uMfT0MF5SKrQCCGQQARoMNTc0MTEwMzc5OTU1Igy/DGuJbWxz5k5RGcUqkQLC93s+zGv8pxc06k858zhvEP3bGKyHV+Nm5rqFZe3KMjnD7owzbZF+KD9Iyn+E06a8MNa/GAwKCgrxnmHzYCt5Ahn/wpWixuNWp7sjqxIct3e/jtOoqikTCrqKMY7zP5Tn9s39DnyCQPKelBojXeNsmWxF3MoZgOQdxv4eHi2XxL220TuKqpQa9Ete3owIjLyzv1V6z4HacLodlg6hK4x4XHimhS2CICFgo2UwxfhbX39PEEjCNj9dAFyi959qqCp1rrsi2eaSmqWSIXfCqfJWYK2ZEFrws4i8UVDkKRoFYA7Zeedq6i4dUi2aQE9Ax401QJ5rAGzMHg1LlZkKxD1FjxC7aNYmBjd7qNpmfZzY8Xcwq4SrwwY6nAEMoSB9VjH6I+qpCOUUt5t/018KAC/pEaGgUe2v6MCaPO910jUR6JAatzX00DEk7jB0JOQyaXeHo29tUq3I0DnWYBHPYMDp09KIQkue3J0uVa8Rb0Esmfc92xLUuHgG2pXeWvOhnd6O0IMr1iEploKojP3NwyDsO9zTOBpQ0v30Mx3i7hQY+LqzPj4BfV96QwFhia2RWMsppD4r3kY=") \
    .getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    print("fs.s3a.connection.timeout:", hadoop_conf.get("fs.s3a.connection.timeout"))
    print("fs.s3a.connection.establish.timeout:", hadoop_conf.get("fs.s3a.connection.establish.timeout"))
    print("fs.s3a.socket.timeout:", hadoop_conf.get("fs.s3a.socket.timeout"))
    print("--------- SPARK CLASSPATH ---------")
    print("\n".join(spark.sparkContext._conf.get("spark.jars").split(",")))
    print("------------------------------------")

    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name)
    
    all_results = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        raw = s3.get_object(Bucket=bucket_name, Key=key)['Body'].read().decode('utf-8')
        print(f"Reading object: {key}")
        print("Raw data preview:", raw[:200]) 
        json_data = json.loads(raw)
        
        comments = extract_comments(json_data)
        analyzed = analyze_sentiment(comments)
        all_results.extend(analyzed)
    
    # Save results
    result_df = spark.createDataFrame(all_results, ["comment", "sentiment_score"])
    result_df.write.mode("overwrite").json("s3a://scalable-youtube-comments-bucket/sentiment-results")

    spark.stop()

if __name__ == "__main__":
    main()

