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
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface KafkaOperations extends AutoCloseable {
  static final Logger logger = LoggerFactory.getLogger(KafkaOperations.class);

  boolean checkConnection(long timeout, TimeUnit unit);

  void restoreGroupOffsets(List<GroupOffsets> offsets, long timeout, TimeUnit unit);

  default void print(GroupOffsets offset) {
    logger.info("Restoring Group:" + offset.getGroup());
    offset
        .getSortedOffset()
        .forEach(
            entry -> {
              TopicPartition topicPartition = entry.getKey();
              OffsetAndMetadata offsetAndMetadata = entry.getValue();
              logger.info(
                  "\tTopic:"
                      + topicPartition.topic()
                      + " Partition:"
                      + topicPartition.partition()
                      + " Offset:"
                      + offsetAndMetadata.offset());
            });
  }
}
