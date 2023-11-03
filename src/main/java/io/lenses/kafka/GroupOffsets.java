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
    if (offsets.isEmpty()) throw new IllegalArgumentException("Offsets cannot be empty");
    this.group = group;
    this.offsets = offsets;
  }

  public String getGroup() {
    return group;
  }

  public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
    return offsets;
  }

  public static void consoleOutput(List<GroupOffsets> offsets) {
    offsets.forEach(
        offset -> {
          System.out.println("Restoring Group:" + offset.getGroup());
          offset
              .getOffsets()
              .forEach(
                  (topicPartition, offsetAndMetadata) -> {
                    System.out.println(
                        "Topic:"
                            + topicPartition.topic()
                            + " Partition:"
                            + topicPartition.partition()
                            + " Offset:"
                            + offsetAndMetadata.offset());
                  });
        });
  }
}
