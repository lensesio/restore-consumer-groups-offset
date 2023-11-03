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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

class AdminClientKafkaOperationsTest {
  @Test
  void returnsTrueOnCheckConnection() {
    Admin admin = mock(Admin.class);
    String clusterId = "clusterId";

    final DescribeClusterResult result = mock(DescribeClusterResult.class);
    when(result.clusterId()).thenReturn(KafkaFuture.completedFuture(clusterId));
    when(admin.describeCluster()).thenReturn(result);
    assertTrue(new AdminClientKafkaOperations(admin).checkConnection(1, TimeUnit.SECONDS));
  }

  @Test
  void returnsFalseOnCheckConnectionWhenTimeoutIsInvolved() {
    Admin admin = mock(Admin.class);

    final DescribeClusterResult result = mock(DescribeClusterResult.class);
    KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
    when(result.clusterId()).thenReturn(future);
    when(admin.describeCluster()).thenReturn(result);
    assertFalse(new AdminClientKafkaOperations(admin).checkConnection(1, TimeUnit.SECONDS));
  }

  @Test
  void appliesTheGroupOffsets() {
    Admin admin = mock(Admin.class);

    AdminClientKafkaOperations ops = new AdminClientKafkaOperations(admin);
    ArrayList<GroupOffsets> offsets = new ArrayList<>();
    offsets.add(
        new GroupOffsets(
            "group",
            Collections.singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(0L, "metadata"))));

    offsets.add(
        new GroupOffsets(
            "group2",
            Collections.singletonMap(
                new TopicPartition("topic2", 0), new OffsetAndMetadata(0L, "metadata"))));

    AlterConsumerGroupOffsetsResult mock1 = mock(AlterConsumerGroupOffsetsResult.class);
    when(mock1.all()).thenReturn(KafkaFuture.completedFuture(null));

    AlterConsumerGroupOffsetsResult mock2 = mock(AlterConsumerGroupOffsetsResult.class);
    when(mock2.all()).thenReturn(KafkaFuture.completedFuture(null));

    when(admin.alterConsumerGroupOffsets(
            "group",
            Collections.singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(0L, "metadata"))))
        .thenReturn(mock1);

    when(admin.alterConsumerGroupOffsets(
            "group2",
            Collections.singletonMap(
                new TopicPartition("topic2", 0), new OffsetAndMetadata(0L, "metadata"))))
        .thenReturn(mock2);
    ops.restoreGroupOffsets(offsets, 1, TimeUnit.SECONDS);

    // check the calls were made once
    verify(admin, times(1))
        .alterConsumerGroupOffsets(
            "group",
            Collections.singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(0L, "metadata")));

    verify(admin, times(1))
        .alterConsumerGroupOffsets(
            "group2",
            Collections.singletonMap(
                new TopicPartition("topic2", 0), new OffsetAndMetadata(0L, "metadata")));
  }
}
