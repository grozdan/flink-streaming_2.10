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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.examples.twitter.util.RabitMqCustom;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONObject;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class TwitterExample {

  public static void main(String[] args) throws Exception {

    args = new String[] {"--output", "C:\\Users\\Grozdan.Madjarov\\Desktop\\result_data.txt",
        "--twitter-source.consumerKey", "2ef02j9Waeo5MpP3dRgI8CjAV", "--twitter-source.consumerSecret",
        "1NcPSr7jgDocfOQZh1J22nniu1ZXqHvAQraYGXTcrwHlOewqsz", "--twitter-source.token",
        "2788099943-FkkfPcAYSXwcvaxou3LykDq2zDYUgQ37WaX3buE", "--twitter-source.tokenSecret",
        "PCya9prxmGe7vUDt1HUFkmY0OuJ7m21YrN5VOHgsXFB9y"};
    final ParameterTool params = ParameterTool.fromArgs(args);
    System.out.println("Usage: TwitterExample [--output <path>] " +
        "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>]");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.getConfig().setGlobalJobParameters(params);

    env.setParallelism(params.getInt("parallelism", 1));

    DataStream<String> streamSource;
    if (params.has(TwitterSource.CONSUMER_KEY) &&
        params.has(TwitterSource.CONSUMER_SECRET) &&
        params.has(TwitterSource.TOKEN) &&
        params.has(TwitterSource.TOKEN_SECRET)
        ) {

      final RMQConnectionConfig connectionConfig =
          new RMQConnectionConfig.Builder()
              .setHost("localhost")
              .setVirtualHost("/")
              .setUserName("guest")
              .setPassword("guest")
              .setPort(5672)
              .build();

      streamSource = env.addSource(new TwitterSource(params.getProperties()));
      DataStream<String> locationMapStream = streamSource.flatMap(new WorldMapDataCreator());
      //locationMapStream.print();
      DataStream<Tuple2<String, Integer>> wordCloudStream = streamSource.flatMap(new WordCloudDataCreator())
          .keyBy(0)
          .timeWindow(Time.seconds(15))
          .sum(1);
      //.filter(tuple -> (Integer) tuple.getField(1) > 3);

      DataStream<String> parsed = wordCloudStream.map(new MapFunction<Tuple2<String, Integer>, String>() {
        @Override
        public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
          JSONObject jsonObject = new JSONObject();
          int size = Integer.parseInt(stringIntegerTuple2.getField(1).toString());
          jsonObject.put("text", stringIntegerTuple2.getField(0).toString());
          jsonObject.put("size", size);
          return jsonObject.toString();
        }
      });
      parsed.print();

      locationMapStream.addSink(new RabitMqCustom<String>(
          connectionConfig,
          "positions5",
          new SimpleStringSchema()));

      parsed.addSink(new RabitMqCustom<>(
          connectionConfig,
          "wordCloud",
          new SimpleStringSchema()));

      env.execute("Twitter Streaming Example");
    }
  }

  public static class WorldMapDataCreator implements FlatMapFunction<String, String> {

    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String sentence, Collector<String> out) throws Exception {
      if (jsonParser == null) {
        jsonParser = new ObjectMapper();
      }
      JsonNode jsonNode = jsonParser.readValue(sentence, JsonNode.class);

      boolean hasText = jsonNode.has("text");
      if (hasText) {
        JSONObject obj = new JSONObject();
        String tweetText = jsonNode.get("text").toString();

        obj.put("name", tweetText);
        if (jsonNode.has("coordinates")) {

          JsonNode coordinates = jsonNode.get("coordinates");
          if (coordinates != null && coordinates.get("coordinates") != null) {
            String str = coordinates.get("coordinates").toString();
            String[] splitCoordinates = str.split(",");
            if (splitCoordinates.length == 2) {
              String longitude = splitCoordinates[0];
              String latitide = splitCoordinates[1];

              obj.put("longitude", Double.parseDouble(longitude.substring(1, longitude.length()).toString()));
              obj.put("latitude", Double.parseDouble(latitide.substring(0, latitide.length() - 1).toString()));
              obj.put("radius", 4);
              //System.err.println(obj.toString());
              //out.collect(obj.toString());
            }
          }
        }
        out.collect(obj.toString());
      }
    }
  }

  public static class WordCloudDataCreator implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private transient ObjectMapper jsonParser;

    /**
     * Select the language from the incoming JSON text
     */
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
      if (jsonParser == null) {
        jsonParser = new ObjectMapper();
      }
      JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

      boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user")
          .get("lang")
          .asText()
          .equals("en");

      boolean hasText = jsonNode.has("text");
      if (isEnglish && hasText) {
        // message of tweet
        String tweetText = jsonNode.get("text").asText();

        StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

        // split the message
        while (tokenizer.hasMoreTokens()) {
          String result = tokenizer.nextToken().trim().toLowerCase();

          if (!result.equals("") && result.length() > 2) {
            out.collect(new Tuple2<>(result, 1));
          }
        }
      }
    }
  }
}


