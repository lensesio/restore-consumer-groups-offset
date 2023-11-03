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

import io.lenses.kafka.AdminClientKafkaOperations;
import io.lenses.kafka.GroupOffsets;
import io.lenses.kafka.KafkaOperations;
import io.lenses.kafka.PreviewAdminClientKafkaOperations;
import io.lenses.s3.AwsGroupOffsetsReader;
import io.lenses.s3.S3AwsGroupOffsetsReader;
import io.lenses.s3.S3ClientBuilderHelper;
import io.lenses.s3.S3Config;
import io.lenses.utils.Ascii;
import io.lenses.utils.Either;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The application will read the group offsets stored in S3. The S3 object key is structured as
 * $bucket/$prefix/${group}/${topic}/${partition} and the content represnets the long bytes of the
 * offset. The application will then restore the group offsets to the Kafka cluster.
 *
 * <p>The application will receive the configuration file as arguments containing:
 *
 * <ul>
 *   <li>source=the AWS bucket and prefix, optionally, where the group offsets are restored
 *   <li>all the Kafka properties are prefixed with kafka
 *   <li>groups=an optional comma separated groups to consider
 *   <li>aws.mode=credentials all default chain provider
 *   <li>aws.region=the target AWS region
 *   <li>aws.access.key=when using credentials mode
 *   <li>aws.secret.key=when using credentials mode
 * </ul>
 */
public class App {
  public static void main(String[] args) {
    Ascii.display("/ascii.txt", System.out::println);
    final Either<Arguments.Errors, Arguments> either = Arguments.from(args);
    if (either.isLeft()) {
      final Arguments.Errors error = either.getLeft();
      System.err.println(error.getMessage());
      switch (error) {
        case MISSING_CONFIG_FILE:
          printUsage();
          break;
        case CONFIG_FILE_DOES_NOT_EXIST:
          break;
      }
      System.exit(1);
    }

    final Arguments arguments = either.getRight();

    try (InputStream inputStream = Files.newInputStream(arguments.getConfigFile().toPath())) {
      final Configuration configuration = Configuration.from(inputStream);
      try (KafkaOperations kafkaOperations =
          arguments.isPreview()
              ? new PreviewAdminClientKafkaOperations()
              : AdminClientKafkaOperations.create(configuration.getKafkaProperties())) {
        if (!kafkaOperations.checkConnection(10, TimeUnit.SECONDS)) {
          System.err.println("Failed to connect to Kafka cluster.");
        } else {
          final S3Config s3Config = configuration.getS3Config();
          try (S3Client s3Client = S3ClientBuilderHelper.build(s3Config)) {
            final AwsGroupOffsetsReader s3Operations = new S3AwsGroupOffsetsReader(s3Client);
            final List<GroupOffsets> offsets =
                s3Operations.read(configuration.getSource(), configuration.getGroups());
            GroupOffsets.consoleOutput(offsets);
            System.out.println("Restoring Groups offsets");
            kafkaOperations.restoreGroupOffsets(offsets, 1, TimeUnit.MINUTES);
            System.out.println("Finished restoring Groups offsets");
          }
        }
      }
    } catch (Exception e) {
      System.err.println("An error occurred. " + e);
      System.exit(1);
    }
  }

  private static void printUsage() {
    System.out.println("Usage: --config <config-file> [--preview]");
  }
}
