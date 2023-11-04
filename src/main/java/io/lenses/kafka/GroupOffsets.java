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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class GroupOffsets {
  private final String group;
  private final Map<TopicPartition, OffsetAndMetadata> offsets;

  public GroupOffsets(String group, Map<TopicPartition, OffsetAndMetadata> offsets) {
    if (group == null) throw new IllegalArgumentException("Group cannot be null");
    if (offsets == null) throw new IllegalArgumentException("Offsets cannot be null");
    this.group = group;
    this.offsets = offsets;
  }

  public String getGroup() {
    return group;
  }

  public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
    return offsets;
  }

  public List<Map.Entry<TopicPartition, OffsetAndMetadata>> getSortedOffset() {
    List<Map.Entry<TopicPartition, OffsetAndMetadata>> sortedOffsets =
        new java.util.ArrayList<>(offsets.entrySet());
    sortedOffsets.sort(new CustomComparator());
    return sortedOffsets;
  }

  private static class CustomComparator
      implements Comparator<Map.Entry<TopicPartition, OffsetAndMetadata>> {
    @Override
    public int compare(
        Map.Entry<TopicPartition, OffsetAndMetadata> left,
        Map.Entry<TopicPartition, OffsetAndMetadata> right) {
      int topicComparison = left.getKey().topic().compareTo(right.getKey().topic());
      if (topicComparison != 0) {
        return topicComparison;
      } else {

        return Integer.compare(left.getKey().partition(), right.getKey().partition());
      }
    }
  }
}
