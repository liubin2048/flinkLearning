package com.liubin.flink.dataStream.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class EventTimeBucketAssigner implements BucketAssigner<String, String> {
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public String getBucketId(String element, Context context) {
    String partitionValue;
    try {
      partitionValue = getPartitionValue(element);
    } catch (Exception e) {
      partitionValue = "00000000";
    }
    return "dt=" + partitionValue;
  }

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }

  private String getPartitionValue(String element) throws Exception {
    JsonNode node = mapper.readTree(element);
    long date = (long) (node.path("timestamp").floatValue() * 1000);
    return new SimpleDateFormat("yyyyMMdd").format(new Date(date));
  }
}
