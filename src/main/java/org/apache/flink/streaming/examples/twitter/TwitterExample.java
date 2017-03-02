/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.twitter;

import java.util.StringTokenizer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Implements the "TwitterStream" program that computes a most used word occurrence over JSON objects in a streaming
 * fashion. <p> The input is a Tweet stream from a TwitterSource. </p> <p> Usage: <code>Usage: TwitterExample [--output
 * <path>] [--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token <token>
 * --twitter-source.tokenSecret <tokenSecret>]</code><br>
 *
 * If no parameters are provided, the program is run with default data from {@link TwitterExampleData}. </p> <p> This
 * example shows how to: <ul> <li>acquire external data, <li>use in-line defined functions, <li>handle flattened stream
 * inputs. </ul>
 */
public class TwitterExample {

  // *************************************************************************
  // PROGRAM
  // *************************************************************************

  public static void main(String[] args) throws Exception {

    args = new String[] {"--output", "C:\\Users\\Grozdan.Madjarov\\Desktop\\result_data.txt",
        "--twitter-source.consumerKey", "2ef02j9Waeo5MpP3dRgI8CjAV", "--twitter-source.consumerSecret",
        "1NcPSr7jgDocfOQZh1J22nniu1ZXqHvAQraYGXTcrwHlOewqsz", "--twitter-source.token",
        "2788099943-FkkfPcAYSXwcvaxou3LykDq2zDYUgQ37WaX3buE", "--twitter-source.tokenSecret",
        "PCya9prxmGe7vUDt1HUFkmY0OuJ7m21YrN5VOHgsXFB9y"};
    final ParameterTool params = ParameterTool.fromArgs(args);
    System.out.println("Usage: TwitterExample [--output <path>] " +
        "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>]");

    // set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    env.setParallelism(params.getInt("parallelism", 1));

    // get input data
    DataStream<String> streamSource;
    if (params.has(TwitterSource.CONSUMER_KEY) &&
        params.has(TwitterSource.CONSUMER_SECRET) &&
        params.has(TwitterSource.TOKEN) &&
        params.has(TwitterSource.TOKEN_SECRET)
        ) {

      streamSource = env.addSource(new TwitterSource(params.getProperties()));

      DataStream<String> dataStream = streamSource;

      dataStream.print();

      final org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig connectionConfig =
          new RMQConnectionConfig.Builder()
              .setHost("localhost")
              .setVirtualHost("/")
              .setUserName("guest")
              .setPassword("guest")
              .setPort(5672)
              .build();

      streamSource.addSink(new RMQSink<String>(
          connectionConfig,            // config for the RabbitMQ connection
          "gs-guide-websocket",                 // name of the RabbitMQ queue to send messages to
          new SimpleStringSchema()));

      env.execute("Twitter Streaming Example");
    }
  }

  public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
      if (jsonParser == null) {
        jsonParser = new ObjectMapper();
      }
      JsonNode jsonNode = jsonParser.readValue(sentence, JsonNode.class);

      boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user")
          .get("lang")
          .asText()
          .equals("en");

      boolean hasText = jsonNode.has("text");
      if (isEnglish && hasText) {

        StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());
        // System.err.println(jsonNode.get("text").asText());
        // split the message
        while (tokenizer.hasMoreTokens()) {
          String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
          out.collect(new Tuple2<String, Integer>(result, 1));
        }
      }
    }
  }
}
