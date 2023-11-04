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

/** A class which only previews the changes made but does not actually make them. */
public class PreviewAdminClientKafkaOperations implements KafkaOperations {
  @Override
  public boolean checkConnection(long timeout, TimeUnit unit) {
    return true;
  }

  @Override
  public void restoreGroupOffsets(List<GroupOffsets> offsets, long timeout, TimeUnit unit) {
    offsets.forEach(
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
        });
  }

  @Override
  public void close() throws Exception {}
}
