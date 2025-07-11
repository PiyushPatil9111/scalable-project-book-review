from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DebugClasspath") \
    .config("spark.jars",
        "/home/ec2-user/spark_clean_jars/hadoop-aws-3.3.4.jar,"
        "/home/ec2-user/spark_clean_jars/aws-java-sdk-core-1.12.99.jar,"
        "/home/ec2-user/spark_clean_jars/aws-java-sdk-s3-1.12.99.jar,"
        "/home/ec2-user/spark_clean_jars/aws-java-sdk-dynamodb-1.12.99.jar") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .getOrCreate()

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
print("fs.s3a.connection.timeout:", hadoop_conf.get("fs.s3a.connection.timeout"))
print("fs.s3a.connection.establish.timeout:", hadoop_conf.get("fs.s3a.connection.establish.timeout"))
print("fs.s3a.socket.timeout:", hadoop_conf.get("fs.s3a.socket.timeout"))
print("--------- SPARK CLASSPATH ---------")
print("\n".join(spark.sparkContext._conf.get("spark.jars").split(",")))
print("-----------------------------------")

spark.stop()
