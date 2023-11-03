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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.lenses.s3.S3Config;
import io.lenses.s3.S3Location;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Optional;

public class Configuration {
  private final S3Location source;
  private final Optional<String[]> groups;

  private final HashMap<String, String> kafkaProperties;

  private final S3Config s3Config;

  public Configuration(
      S3Location source,
      Optional<String[]> groups,
      S3Config s3Config,
      HashMap<String, String> kafkaProperties) {
    if (source == null) throw new IllegalArgumentException("S3 source cannot be null");
    if (s3Config == null) throw new IllegalArgumentException("S3 config cannot be null");
    if (kafkaProperties == null)
      throw new IllegalArgumentException("Kafka properties cannot be null");
    this.source = source;
    this.groups = groups;
    this.kafkaProperties = kafkaProperties;
    this.s3Config = s3Config;
  }

  public S3Location getSource() {
    return source;
  }

  public Optional<String[]> getGroups() {
    return groups;
  }

  public HashMap<String, String> getKafkaProperties() {
    return kafkaProperties;
  }

  public static Configuration from(InputStream inputStream) {
    // read the input stream as HOCON and return the configuration
    final Config config = ConfigFactory.parseReader(new InputStreamReader(inputStream));
    // read all kafka properties
    final HashMap<String, String> kafkaProperties = new HashMap<>();
    if (!config.hasPath("kafka"))
      throw new IllegalArgumentException("Kafka properties are required");
    config
        .getConfig("kafka")
        .entrySet()
        .forEach(e -> kafkaProperties.put(e.getKey(), e.getValue().unwrapped().toString()));

    // read the source
    if (!config.hasPath("aws")) throw new IllegalArgumentException("S3 source is required");
    final Config sourceConfig = config.getConfig("aws");
    if (!sourceConfig.hasPath("bucket"))
      throw new IllegalArgumentException("S3 bucket is required");
    final String bucket = sourceConfig.getString("bucket");
    final Optional<String> prefix = Optional.ofNullable(sourceConfig.getString("prefix"));
    final S3Location source = new S3Location(bucket, prefix);

    // groups are optional, when define it's a comma separated list
    final Optional<String[]> groups =
        config.hasPath("groups")
            ? Optional.of(config.getString("groups").split(","))
            : Optional.empty();

    // read AwsMode
    final S3Config s3Config = S3Config.from(config);
    return new Configuration(source, groups, s3Config, kafkaProperties);
  }

  public S3Config getS3Config() {
    return s3Config;
  }
}
