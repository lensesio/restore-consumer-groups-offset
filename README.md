# restore-consumer-groups-offset

An application allowing to read the consumer groups offsets stored in S3 and apply them to the target Kafka cluster.
The S3 sink stores the information under the following path: `bucket[/prefix]/group/topic/partition`, with the content
being the offset (8 bytes array).

### Configuration

The application requires the configuration file. The configuration file in HOCON format supports the following options:

| Configuration Option      | Description                                                                                                                                                          |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Kafka configuration`     | All Kafka settings should be prefixed with `kafka.`. For example: `kafka.bootstrap.servers = "localhost:9092"`.                                                      |
| `S3 location`             | - `aws.bucket`: The name of the S3 bucket where consumer group offsets are stored.                                                                                   |
|                           | - `aws.prefix` (Optional): The prefix of objects within the bucket.                                                                                                  |
| `groups`                  | An optional, comma-separated list of consumer groups to restore. If not specified, all groups stored in S3 will be restored. For example: `groups = group1, group2`. |
| `AWS connection mode`     | - `aws.mode`: Set to `credentials` to use provided credentials or `default` for AWS default credentials provider chain.                                              |
| `AWS Access Key`          | - `aws.access.key`: AWS access key ID (only when `aws.mode` is set to `credentials`).                                                                                |
| `AWS Secret Key`          | - `aws.secret.key`: AWS secret access key (only when `aws.mode` is `credentials`).                                                                                   |
| `AWS Region`              | - `aws.region`: AWS region (only when `aws.mode` is `credentials`).                                                                                                  |
| `AWS HTTP Retries`        | - `aws.http.retries`: How many times a failed request is attempted. Default is 5                                                                                     |
| `AWS HTTP Retry interval` | - `aws.http.retry.inteval`: The time in milliseconds to wait before an HTTP operation is retried. Default is 50.                                                     |


#### Examples

##### Example 1

```hocon
kafka {
  bootstrap.servers = "localhost:9092"
  # Add all the kafka settings required to connect to your target cluster
}

# Comma separated list of groups stored in S3 to consider
#groups=groupa,groupb,groupc 

aws {
  # The name of the S3 bucket where consumer groups offsets are stored.
  bucket = "replace-me"
  ## Optional prefix when the data was stored in a bucket and prefix.
  #prefix = "prefix1/prefix2"

  # The AWS connection mode. It can be "default" or "credentials".
  # When using "default" the default AWS credentials provider chain is used.
  # When using "credentials" the access.key and secret.key properties must be set
  mode = "default"
  # only used when mode is "credentials"
  #access.key = "replace-me"

  # only used when mode is "credentials"
  #secret.key = "replace-me"

  # The AWS region where the S3 bucket is located.
  region = "eu-west-2"
}
```

##### Example 2

```hocon
kafka.bootstrap.servers = "localhost:9092"

aws.bucket = "your-s3-bucket"
aws.prefix = "optional-prefix"

groups = "group1,group2"

aws.mode = "credentials" # or "default"
aws.access.key = "your-access-key"
aws.secret.key = "your-secret-key"
aws.region = "your-aws-region"
```

## Running the application

It requires at least Java 8 to run. 
To run the application, download the latest release unpack the archive, locate the bin folder and run the following command:

```bash 
chmod 777 restore.sh

# update the config file with your settings
restore.sh --config config.conf
```



To format the code run:

```bash
mvn com.coveo:fmt-maven-plugin:format
```

To add license header, run:

```bash
mvn license:format
```