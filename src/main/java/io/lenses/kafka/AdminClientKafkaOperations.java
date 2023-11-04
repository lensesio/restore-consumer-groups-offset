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
package io.lenses.kafka;

import io.lenses.utils.Tuple2;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;

/** A class which uses the AdminClient to store the consumer groups offsets. */
public class AdminClientKafkaOperations implements KafkaOperations {
  private final Admin admin;

  public AdminClientKafkaOperations(Admin adminClient) {
    if (adminClient == null) throw new IllegalArgumentException("AdminClient cannot be null");
    this.admin = adminClient;
  }

  /**
   * Checks the connection to the Kafka cluster
   *
   * @return true if the connection is successful, false otherwise
   */
  @Override
  public boolean checkConnection(long timeout, TimeUnit unit) {
    try {
      DescribeClusterResult result = admin.describeCluster();
      result.clusterId().get(timeout, unit);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void restoreGroupOffsets(List<GroupOffsets> offsets, long timeout, TimeUnit unit) {
    // traverse the list of GroupOffsets and call the admin client to restore the offsets
    // capture the future and wait for it to complete
    // if the future fails, throw an exception
    List<Tuple2<GroupOffsets, AlterConsumerGroupOffsetsResult>> results =
        offsets.stream()
            .map(
                offset -> {
                  System.out.println("Restoring Group:" + offset.getGroup());
                  offset
                      .getOffsets()
                      .forEach(
                          (topicPartition, offsetAndMetadata) -> {
                            System.out.println(
                                "\tTopic:"
                                    + topicPartition.topic()
                                    + " Partition:"
                                    + topicPartition.partition()
                                    + " Offset:"
                                    + offsetAndMetadata.offset());
                          });
                  AlterConsumerGroupOffsetsResult result =
                      admin.alterConsumerGroupOffsets(offset.getGroup(), offset.getOffsets());
                  return new Tuple2<>(offset, result);
                })
            .collect(Collectors.toList());

    results.forEach(
        result -> {
          try {
            System.out.println("Awaiting result for group:" + result._1().getGroup());
            result._2().all().get(timeout, unit);
          } catch (Exception e) {
            throw new RuntimeException(
                "Failed to restore group offsets for group:" + result._1().getGroup(), e);
          }
        });
  }

  @Override
  public void close() throws Exception {
    admin.close();
  }

  public static AdminClientKafkaOperations create(Map<String, String> properties) {
    final Properties props = new Properties();
    props.putAll(properties);
    return create(props);
  }

  public static AdminClientKafkaOperations create(Properties properties) {
    if (properties == null) throw new IllegalArgumentException("Properties cannot be null");
    AdminClient adminClient = AdminClient.create(properties);
    return new AdminClientKafkaOperations(adminClient);
  }
}
