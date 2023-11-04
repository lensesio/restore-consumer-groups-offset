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
  private final S3Client s3Client;

  public S3AwsGroupOffsetsReader(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  @Override
  public List<GroupOffsets> read(S3Location source, Optional<String[]> groups) {
    System.out.println(
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
    System.out.println("Reading offsets from S3...");
    while (iterator.hasNext()) {
      final ListObjectsV2Response response = iterator.next();
      for (S3Object s3Object : response.contents()) {
        String key = s3Object.key();
        System.out.println("\tkey:" + key);
        final ResponseBytes<GetObjectResponse> objResponse =
            s3Client.getObjectAsBytes(
                GetObjectRequest.builder().bucket(source.getBucket()).key(key).build());
        final long offset = objResponse.asByteBuffer().getLong();
        final String[] parts = key.split("/");
        final String group = parts[parts.length - 3];
        final String topic = parts[parts.length - 2];
        final int partition = Integer.parseInt(parts[parts.length - 1]);
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
        if (groups.isPresent()) {
          final String[] groupsArray = groups.get();

          if (Arrays.asList(groupsArray).contains(group)) {
            offsetsMap
                .computeIfAbsent(group, k -> new GroupOffsets(group, new HashMap<>()))
                .getOffsets()
                .put(topicPartition, offsetAndMetadata);
          }
        } else {
          offsetsMap
              .computeIfAbsent(group, k -> new GroupOffsets(group, new HashMap<>()))
              .getOffsets()
              .put(topicPartition, offsetAndMetadata);
        }
      }
    }
    final List<GroupOffsets> groupsOffsets = new ArrayList<>(offsetsMap.values());
    groupsOffsets.sort(Comparator.comparing(GroupOffsets::getGroup));
    System.out.println(
        "Finished reading Consumer Groups offsets S3 data. Found "
            + groupsOffsets.size()
            + " groups.");
    return groupsOffsets;
  }
}
