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

import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

public class S3ClientBuilderHelper {
  public static S3Client build(S3Config config) {
    // build the S3 client based on the configuration
    final RetryPolicy retryPolicy =
        RetryPolicy.builder()
            .numRetries(config.getAwsHttpRetries())
            .backoffStrategy(
                FixedDelayBackoffStrategy.create(
                    Duration.ofMillis(config.getAwsHttpRetryInterval())))
            .build();

    final ClientOverrideConfiguration overrideConfig =
        ClientOverrideConfiguration.builder().retryPolicy(retryPolicy).build();

    final S3Configuration s3Config =
        S3Configuration.builder()
            .pathStyleAccessEnabled(config.isEnableVirtualHostBuckets())
            .build();

    final SdkHttpClient httpClient = ApacheHttpClient.builder().build();

    final AwsCredentialsProvider credsProv = credentialsProvider(config);
    final S3ClientBuilder builder =
        S3Client.builder()
            .overrideConfiguration(overrideConfig)
            .serviceConfiguration(s3Config)
            .credentialsProvider(credsProv)
            .httpClient(httpClient);
    if (config.getAwsRegion().isPresent()) {
      builder.region(Region.of(config.getAwsRegion().get()));
    }
    return builder.build();
  }

  private static AwsCredentialsProvider credentialsProvider(S3Config config) {
    switch (config.getAwsMode()) {
      case CREDENTIALS:
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(
                config.getAwsAccessKey().get(), config.getAwsSecretKey().get()));
      case DEFAULT:
        return DefaultCredentialsProvider.create();
      default:
        throw new IllegalArgumentException("Unsupported AWS mode: " + config.getAwsMode());
    }
  }
}
