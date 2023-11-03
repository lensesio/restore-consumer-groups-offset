/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at: http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package io.lenses;

import static org.junit.jupiter.api.Assertions.*;

import io.lenses.s3.AwsMode;
import java.io.ByteArrayInputStream;
import org.junit.jupiter.api.Test;

class ConfigurationTest {

  // code to test
  /* public static Configuration from(InputStream inputStream) {
      //read the input stream as HOCON and return the configuration
      final Config config = ConfigFactory.parseReader(new InputStreamReader(inputStream));
      //read all kafka properties
      final HashMap<String, String> kafkaProperties = new HashMap<>();
      if (!config.hasPath("kafka")) throw new IllegalArgumentException("Kafka properties are required");
      config.getConfig("kafka").entrySet().forEach(e -> kafkaProperties.put(e.getKey(), e.getValue().unwrapped().toString()));

      //read the source
      if (!config.hasPath("aws")) throw new IllegalArgumentException("S3 source is required");
      final Config sourceConfig = config.getConfig("aws");
      if (!sourceConfig.hasPath("bucket")) throw new IllegalArgumentException("S3 bucket is required");
      final String bucket = sourceConfig.getString("bucket");
      final Optional<String> prefix = Optional.ofNullable(sourceConfig.getString("prefix"));
      final S3Location source = new S3Location(bucket, prefix);

      final Optional<String[]> groups = Optional.of(config.getString("groups").split(","));

      //read AwsMode
      final AwsMode awsMode = AwsMode.valueOf(config.getString("aws.mode").toUpperCase());
      final String awsRegion = config.getString("aws.region");
      //if credentials mode, read the access and secret keys
      final Optional<String> awsAccessKey = awsMode == AwsMode.CREDENTIALS ? Optional.of(config.getString("aws.access.key")) : Optional.empty();
      final Optional<String> awsSecretKey = awsMode == AwsMode.CREDENTIALS ? Optional.of(config.getString("aws.secret.key")) : Optional.empty();

      return new Configuration(source, groups, awsMode, awsRegion, awsAccessKey, awsSecretKey, kafkaProperties);
  } */
  @Test
  void createAnInstanceOfConfigurationFromTheHoconConfiguration() {
    // create the Hocon configuration as string; use flatten keys
    final String hocon =
        "kafka.bootstrap.servers=\"localhost:9092\"\n"
            + "kafka.security.protocol=PLAINTEXT\n"
            + "kafka.sasl.mechanism=PLAIN\n"
            + "kafka.sasl.jaas.config=\"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";\"\n"
            + "aws.bucket=io.lenses\n"
            + "aws.prefix=group-offsets\n"
            + "groups=\"group1,group2\"\n"
            + "aws.mode=credentials\n"
            + "aws.region=eu-west-1\n"
            + "aws.access.key=access-key\n"
            + "aws.secret.key=secret-key\n";

    // println hocon
    System.out.println(hocon);
    final Configuration configuration =
        Configuration.from(new ByteArrayInputStream(hocon.getBytes()));
    assertNotNull(configuration);
    assertEquals("io.lenses", configuration.getSource().getBucket());
    assertEquals("group-offsets", configuration.getSource().getPrefix().get());
    assertEquals("group1", configuration.getGroups().get()[0]);
    assertEquals("group2", configuration.getGroups().get()[1]);
    assertEquals(AwsMode.CREDENTIALS, configuration.getS3Config().getAwsMode());
    assertEquals("eu-west-1", configuration.getS3Config().getAwsRegion());
    assertEquals("access-key", configuration.getS3Config().getAwsAccessKey().get());
    assertEquals("secret-key", configuration.getS3Config().getAwsSecretKey().get());
    assertEquals("localhost:9092", configuration.getKafkaProperties().get("bootstrap.servers"));
    assertEquals("PLAINTEXT", configuration.getKafkaProperties().get("security.protocol"));
    assertEquals("PLAIN", configuration.getKafkaProperties().get("sasl.mechanism"));
    assertEquals(
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=admin password=admin-secret;",
        configuration.getKafkaProperties().get("sasl.jaas.config"));
  }

  @Test
  void ignoreSecretAndAccessKeysIfTheModeIsDefault() {
    final String hocon =
        "kafka.bootstrap.servers=\"localhost:9092\"\n"
            + "kafka.security.protocol=PLAINTEXT\n"
            + "kafka.sasl.mechanism=PLAIN\n"
            + "kafka.sasl.jaas.config=\"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";\"\n"
            + "aws.bucket=io.lenses\n"
            + "aws.prefix=group-offsets\n"
            + "groups=\"group1,group2\"\n"
            + "aws.mode=default\n"
            + "aws.region=eu-west-1\n"
            + "aws.access.key=access-key\n"
            + "aws.secret.key=secret-key\n";

    final Configuration configuration =
        Configuration.from(new ByteArrayInputStream(hocon.getBytes()));
    assertNotNull(configuration);
    assertEquals("io.lenses", configuration.getSource().getBucket());
    assertEquals("group-offsets", configuration.getSource().getPrefix().get());
    assertEquals("group1", configuration.getGroups().get()[0]);
    assertEquals("group2", configuration.getGroups().get()[1]);
    assertEquals(AwsMode.DEFAULT, configuration.getS3Config().getAwsMode());
    assertEquals("eu-west-1", configuration.getS3Config().getAwsRegion());
    assertFalse(configuration.getS3Config().getAwsAccessKey().isPresent());
    assertFalse(configuration.getS3Config().getAwsSecretKey().isPresent());
    assertEquals("localhost:9092", configuration.getKafkaProperties().get("bootstrap.servers"));
    assertEquals("PLAINTEXT", configuration.getKafkaProperties().get("security.protocol"));
    assertEquals("PLAIN", configuration.getKafkaProperties().get("sasl.mechanism"));
    assertEquals(
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=admin password=admin-secret;",
        configuration.getKafkaProperties().get("sasl.jaas.config"));
  }

  @Test
  void hasEmptyGroupsWhenTheGroupSettingIsNotSpecified() {
    final String hocon =
        "kafka.bootstrap.servers=\"localhost:9092\"\n"
            + "kafka.security.protocol=PLAINTEXT\n"
            + "kafka.sasl.mechanism=PLAIN\n"
            + "kafka.sasl.jaas.config=\"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";\"\n"
            + "aws.bucket=io.lenses\n"
            + "aws.prefix=group-offsets\n"
            + "aws.mode=default\n"
            + "aws.region=eu-west-1\n"
            + "aws.access.key=access-key\n"
            + "aws.secret.key=secret-key\n";

    final Configuration configuration =
        Configuration.from(new ByteArrayInputStream(hocon.getBytes()));
    assertNotNull(configuration);
    assertEquals("io.lenses", configuration.getSource().getBucket());
    assertEquals("group-offsets", configuration.getSource().getPrefix().get());
    assertFalse(configuration.getGroups().isPresent());
    assertEquals(AwsMode.DEFAULT, configuration.getS3Config().getAwsMode());
    assertEquals("eu-west-1", configuration.getS3Config().getAwsRegion());
    assertFalse(configuration.getS3Config().getAwsAccessKey().isPresent());
    assertFalse(configuration.getS3Config().getAwsSecretKey().isPresent());
    assertEquals("localhost:9092", configuration.getKafkaProperties().get("bootstrap.servers"));
    assertEquals("PLAINTEXT", configuration.getKafkaProperties().get("security.protocol"));
    assertEquals("PLAIN", configuration.getKafkaProperties().get("sasl.mechanism"));
    assertEquals(
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=admin password=admin-secret;",
        configuration.getKafkaProperties().get("sasl.jaas.config"));
  }

  @Test
  void throwsAnExceptionWhenCredentialModeIsSetButSecretKeyIsNot() {
    final String hocon =
        "kafka.bootstrap.servers=\"localhost:9092\"\n"
            + "kafka.security.protocol=PLAINTEXT\n"
            + "kafka.sasl.mechanism=PLAIN\n"
            + "kafka.sasl.jaas.config=\"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";\"\n"
            + "aws.bucket=io.lenses\n"
            + "aws.prefix=group-offsets\n"
            + "aws.mode=credentials\n"
            + "aws.region=eu-west-1\n"
            + "aws.access.key=access-key\n";

    assertThrows(
        IllegalArgumentException.class,
        () -> Configuration.from(new ByteArrayInputStream(hocon.getBytes())));
  }

  @Test
  void throwsAnExceptionWhenCredentialModeIsSetButAccessKeyIsNot() {
    final String hocon =
        "kafka.bootstrap.servers=\"localhost:9092\"\n"
            + "kafka.security.protocol=PLAINTEXT\n"
            + "kafka.sasl.mechanism=PLAIN\n"
            + "kafka.sasl.jaas.config=\"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";\"\n"
            + "aws.bucket=io.lenses\n"
            + "aws.prefix=group-offsets\n"
            + "aws.mode=credentials\n"
            + "aws.region=eu-west-1\n"
            + "aws.secret.key=secret-key\n";

    assertThrows(
        IllegalArgumentException.class,
        () -> Configuration.from(new ByteArrayInputStream(hocon.getBytes())));
  }
}
