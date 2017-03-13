package org.apache.flink.streaming.examples.twitter.util;

import java.io.IOException;
import java.util.Map;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Created by Grozdan.Madjarov on 3/8/2017.
 */
public class RabitMqCustom<IN> extends RMQSink<IN> {
  public RabitMqCustom(RMQConnectionConfig rmqConnectionConfig,
      String queueName, SerializationSchema schema) {
    super(rmqConnectionConfig, queueName, schema);
  }

  @Override
  protected void setupQueue() throws IOException {
    this.channel.queueDeclare(this.queueName, true, false, false, (Map) null);
  }
}
