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
package io.lenses.s3;

import com.typesafe.config.Config;
import java.util.Optional;

public class S3Config {
  private final AwsMode awsMode;
  private final Optional<String> awsRegion;

  private final Optional<String> awsAccessKey;
  private final Optional<String> awsSecretKey;

  private final int awsHttpRetries;
  private final long awsHttpRetryInterval;

  private final boolean enableVirtualHostBuckets;

  public S3Config(
      AwsMode awsMode,
      Optional<String> awsRegion,
      Optional<String> awsAccessKey,
      Optional<String> awsSecretKey,
      int awsHttpRetries,
      long awsHttpRetryInterval,
      boolean enableVirtualHostBuckets) {
    if (awsMode == null) throw new IllegalArgumentException("AWS mode cannot be null");
    if (awsRegion == null) throw new IllegalArgumentException("AWS region cannot be null");
    if (awsMode == AwsMode.CREDENTIALS
        && (!awsAccessKey.isPresent() || !awsSecretKey.isPresent())) {
      throw new IllegalArgumentException("AWS credentials mode requires access and secret keys");
    }
    this.awsMode = awsMode;
    this.awsRegion = awsRegion;
    this.awsAccessKey = awsAccessKey;
    this.awsSecretKey = awsSecretKey;
    this.awsHttpRetries = awsHttpRetries;
    this.awsHttpRetryInterval = awsHttpRetryInterval;
    this.enableVirtualHostBuckets = enableVirtualHostBuckets;
  }

  public AwsMode getAwsMode() {
    return awsMode;
  }

  public Optional<String> getAwsRegion() {
    return awsRegion;
  }

  public Optional<String> getAwsAccessKey() {
    return awsAccessKey;
  }

  public Optional<String> getAwsSecretKey() {
    return awsSecretKey;
  }

  public int getAwsHttpRetries() {
    return awsHttpRetries;
  }

  public long getAwsHttpRetryInterval() {
    return awsHttpRetryInterval;
  }

  public static S3Config from(Config config) {
    final AwsMode awsMode = AwsMode.valueOf(config.getString("aws.mode").toUpperCase());
    final String awsRegion = config.getString("aws.region");
    // if credentials mode, read the access and secret keys or throw an exception if they are
    // missing
    if (awsMode == AwsMode.CREDENTIALS
        && (!config.hasPath("aws.access.key") || !config.hasPath("aws.secret.key"))) {
      throw new IllegalArgumentException("AWS credentials mode requires access and secret keys");
    }
    final Optional<String> awsAccessKey =
        awsMode == AwsMode.CREDENTIALS
            ? Optional.of(config.getString("aws.access.key"))
            : Optional.empty();
    final Optional<String> awsSecretKey =
        awsMode == AwsMode.CREDENTIALS
            ? Optional.of(config.getString("aws.secret.key"))
            : Optional.empty();

    // read the http retries and interval if not default to 5 and 50L
    final int awsHttpRetries =
        config.hasPath("aws.http.retries") ? config.getInt("aws.http.retries") : 5;
    final long awsHttpRetryInterval =
        config.hasPath("aws.http.retry.interval") ? config.getLong("aws.http.retry.interval") : 50L;

    final boolean enableVirtualHostBuckets =
        config.hasPath("aws.enable.virtual.host.buckets")
            && config.getBoolean("aws.enable.virtual.host.buckets");
    return new S3Config(
        awsMode,
        Optional.of(awsRegion),
        awsAccessKey,
        awsSecretKey,
        awsHttpRetries,
        awsHttpRetryInterval,
        enableVirtualHostBuckets);
  }

  public boolean isEnableVirtualHostBuckets() {
    return enableVirtualHostBuckets;
  }
}
