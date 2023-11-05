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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.lenses.utils.Tuple2;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class S3AwsGroupOffsetsReaderTest {

  @Test
  void extractGroupTopicPartition() {
    String key = "group/topic/0";
    Tuple2<String, TopicPartition> result = S3AwsGroupOffsetsReader.extractGroupTopicPartition(key);
    assertEquals("group", result._1());
    assertEquals("topic", result._2().topic());
    assertEquals(0, result._2().partition());
  }

  @Test
  void throwsAnIllegalArgumentExceptionWhenThePartitionIsNotANumber() {
    String key = "group/topic/abc";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> S3AwsGroupOffsetsReader.extractGroupTopicPartition(key));
    assertEquals("Invalid S3 key:group/topic/abc", exception.getMessage());
  }

  @Test
  void throwsIllegalArgumentWhenTheKeyIsNotGroupTopicPartition() {
    String key = "group/topic";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> S3AwsGroupOffsetsReader.extractGroupTopicPartition(key));
    assertEquals("Invalid S3 key:group/topic", exception.getMessage());
  }

  @Test
  void extractGroupTopicPartitionWhenPrefixIsPresent() {
    String key = "prefix/group/topic/0";
    Tuple2<String, TopicPartition> result = S3AwsGroupOffsetsReader.extractGroupTopicPartition(key);
    assertEquals("group", result._1());
    assertEquals("topic", result._2().topic());
    assertEquals(0, result._2().partition());
  }

  @Test
  void extractGroupTopicPartitionWhenNestedPrefixIsPresent() {
    String key = "prefix1/prefix2/group/topic/0";
    Tuple2<String, TopicPartition> result = S3AwsGroupOffsetsReader.extractGroupTopicPartition(key);
    assertEquals("group", result._1());
    assertEquals("topic", result._2().topic());
    assertEquals(0, result._2().partition());
  }
}
