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
