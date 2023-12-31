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

import io.lenses.kafka.GroupOffsets;
import io.lenses.utils.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * Implementation of {@link AwsGroupOffsetsReader} that reads the offsets from S3.
 *
 * <p>The S3 sink stores consumer groups offsets using a key like:
 * bucket/prefix/${group}/${topic}/${partition}. The content is the 8 bytes long of the offset. The
 * implementation starts from the bucket and prefix, and then it will list all the groups, topics
 * and partitions and read the offsets.
 */
public class S3AwsGroupOffsetsReader implements AwsGroupOffsetsReader {
  private static final Logger logger = LoggerFactory.getLogger(S3AwsGroupOffsetsReader.class);
  private final S3Client s3Client;

  public S3AwsGroupOffsetsReader(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public List<GroupOffsets> read(S3Location source, Optional<String[]> groups) {
    logger.info(
        "Reading Consumer Group offsets from bucket:"
            + source.getBucket()
            + " prefix:"
            + source.getPrefix().orElse(""));
    ListObjectsV2Request.Builder requestBuilder =
        ListObjectsV2Request.builder().bucket(source.getBucket());
    if (source.getPrefix().isPresent()) {
      requestBuilder.prefix(source.getPrefix().get());
    }
    ListObjectsV2Request request = requestBuilder.build();
    ListObjectsV2Iterable iterable = s3Client.listObjectsV2Paginator(request);
    final Iterator<ListObjectsV2Response> iterator = iterable.iterator();
    final Map<String, GroupOffsets> offsetsMap = new HashMap<>();
    logger.info("Reading offsets from S3...");
    while (iterator.hasNext()) {
      final ListObjectsV2Response response = iterator.next();
      for (S3Object s3Object : response.contents()) {
        String key = s3Object.key();
        if (!isValidKey(key)) {
          continue;
        }
        logger.info("\tkey:" + key);
        final ResponseBytes<GetObjectResponse> objResponse =
            s3Client.getObjectAsBytes(
                GetObjectRequest.builder().bucket(source.getBucket()).key(key).build());
        final long offset = objResponse.asByteBuffer().getLong();
        final Tuple2<String, TopicPartition> groupTopicPartition = extractGroupTopicPartition(key);
        final String group = groupTopicPartition._1();
        final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
        if (groups.isPresent()) {
          final String[] groupsArray = groups.get();

          if (Arrays.asList(groupsArray).contains(group)) {
            offsetsMap
                .computeIfAbsent(group, k -> new GroupOffsets(group, new HashMap<>()))
                .getOffsets()
                .put(groupTopicPartition._2(), offsetAndMetadata);
          }
        } else {
          offsetsMap
              .computeIfAbsent(group, k -> new GroupOffsets(group, new HashMap<>()))
              .getOffsets()
              .put(groupTopicPartition._2(), offsetAndMetadata);
        }
      }
    }
    final List<GroupOffsets> groupsOffsets = new ArrayList<>(offsetsMap.values());
    groupsOffsets.sort(Comparator.comparing(GroupOffsets::getGroup));
    logger.info(
        "Finished reading Consumer Groups offsets S3 data. Found "
            + groupsOffsets.size()
            + " groups.");
    return groupsOffsets;
  }

  /**
   * Extracts the group, topic and partition from the S3 key. The S3 key is structured as
   * ../${group}/${topic}/${partition}
   *
   * @param key the S3 key
   * @return a tuple of group and topic partition
   */
  public static Tuple2<String, TopicPartition> extractGroupTopicPartition(String key) {
    final String[] parts = key.split("/");
    // if parts is not at least 3, then the key is not valid
    if (parts.length < 3) {
      throw new IllegalArgumentException("Invalid S3 key:" + key);
    }
    final String group = parts[parts.length - 3];
    final String topic = parts[parts.length - 2];
    try {
      final int partition = Integer.parseInt(parts[parts.length - 1]);
      return new Tuple2<>(group, new TopicPartition(topic, partition));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid S3 key:" + key, e);
    }
  }

  private static boolean isValidKey(String s3Key) {
    final String[] parts = s3Key.split("/");
    // if parts is not at least 3, then the key is not valid
    if (parts.length < 3) {
      return false;
    }
    try {
      Integer.parseInt(parts[parts.length - 1]);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
