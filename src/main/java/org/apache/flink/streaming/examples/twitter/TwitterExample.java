
package org.apache.flink.streaming.examples.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.examples.twitter.util.RabitMqCustom;
import org.apache.flink.streaming.examples.twitter.util.UtilsStatic;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class TwitterExample {
  public static int counter = 0;
  public static final int WORDS_IN_CLOUD = 50;
  public static final int WORD_CLOUD_WINDOW_SECONDS = 20;
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

      locationMapStream.addSink(new RabitMqCustom<String>(
          connectionConfig,
          "positions10",
          new SimpleStringSchema()));

      //locationMapStream.print();
      DataStream<Tuple2<String, Integer>> wordCloudStream = streamSource.flatMap(new WordCloudDataCreator())
          .keyBy(0)
          .timeWindow(Time.seconds(WORD_CLOUD_WINDOW_SECONDS))
          .sum(1);

      //wordCloudStream.print();

      AllWindowedStream<Tuple2<String, Integer>, TimeWindow> windowWordCLoud =
          wordCloudStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(WORD_CLOUD_WINDOW_SECONDS)));

      DataStream<String> wordCloud =
          windowWordCLoud.apply(new AllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> wordsFrequencies,
                Collector<String> collector) throws Exception {
              List<Tuple2<String, Integer>> sortedWordsFrequncies = new ArrayList<Tuple2<String, Integer>>();

              for (Tuple2<String, Integer> wordFrequency : wordsFrequencies) {
                sortedWordsFrequncies.add(wordFrequency);
              }

              sortedWordsFrequncies.sort(new Comparator<Tuple2<String, Integer>>() {
                @Override
                public int compare(Tuple2<String, Integer> word1, Tuple2<String, Integer> word2) {
                  return word2.f1 - word1.f1;
                }
              });

              String wordCloud = sortedWordsFrequncies.stream()

                  //.map(word -> {
                  //  JSONObject jsonObject = new JSONObject();
                  //  int size = Integer.parseInt(word.getField(1).toString());
                  //  try {
                  //    jsonObject.put("text", word.getField(0).toString());
                  //    jsonObject.put("size", size * 10);
                  //    return jsonObject.toString();
                  //  } catch (JSONException e) {
                  //    e.printStackTrace();
                  //  }
                  //  return null;
                  //})
                  .filter(wordFrequency -> {
                    String word = wordFrequency.getField(0);
                    if (!UtilsStatic.getStopWords().contains(word)) {
                      return true;
                    }
                    return false;
                  })
                  .limit(WORDS_IN_CLOUD)
                  .map(wordFrequency -> wordFrequency.toString())//delete this later
                  .collect(Collectors.joining("."));
              putWordCloudInDatabase(wordCloud);
              collector.collect(wordCloud);
            }
          });

      //wordCloud.print();
      //wordCloud.addSink(new RabitMqCustom<String>(
      //    connectionConfig,
      //    "wordCloud4",
      //    new SimpleStringSchema()));
      env.execute("Twitter Streaming Example");
    }
  }

  private static java.sql.Timestamp getCurrentTimeStamp() {

    java.util.Date today = new java.util.Date();
    return new java.sql.Timestamp(today.getTime());
  }

  private static void putWordCloudInDatabase(String wordCloud) throws JSONException {
    wordCloud = formatTuples(wordCloud);

    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
      Connection con = DriverManager.getConnection(
          "jdbc:mysql://localhost:3306/apache-flink-db?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false",
          "root", "");
      String query = " insert into word_cloud_model (date_time, tweets_per_country)"
          + " values (?, ?)";
      PreparedStatement preparedStmt = con.prepareStatement(query);
      preparedStmt.setTimestamp(1, getCurrentTimeStamp());
      preparedStmt.setString(2, wordCloud);

      preparedStmt.execute();

      con.close();
    } catch (Exception e) {
      System.out.println("EXCEPTION KAJ WORDCLOUD\n" + e);
    }
  }

  private static String formatTuples(String wordCloud) throws JSONException {
    String[] tuples = wordCloud.split("\\.");
    JSONObject returnObj = new JSONObject();
    int counter = 0;
    for (String tuple : tuples) {
      tuple = tuple.replaceAll("[()]", "");
      String[] keyValue = tuple.split(",");
      if (keyValue.length == 2) {
        String word = keyValue[0];
        Integer size = Integer.parseInt(keyValue[1]);
        //if (!UtilsStatic.getStopWords().contains(word)) {
        returnObj.put(word, size);
        //counter++;
        //}
        //if (counter == WORDS_IN_CLOUD) {
        //  break;
        //}
      }
    }
    return returnObj.toString();
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
        JSONObject jsono = createObjectFromTweet(jsonNode.get("place"));
        jsono.put("name", tweetText);
        if (jsono.has("latitude")) {
          boolean result =
              putTweetInDatabase(tweetText, (Double) jsono.get("latitude"), (Double) jsono.get("longitude"));
          if (!result) {
            result = putTweetInDatabase("", (Double) jsono.get("latitude"), (Double) jsono.get("longitude"));
          }
          if (result) {
            counter++;
            System.err.println(counter + ". " + jsono.toString());
            out.collect(jsono.toString());
          }
        }
      }
    }

    private JSONObject createObjectFromTweet(JsonNode place) {
      JSONObject obj = new JSONObject();
      try {
        if (place != null && !place.toString().equals("null")) {
          String coordinates = place.get("bounding_box").get("coordinates").get(0).get(0).toString();
          String[] splitCoordinates = coordinates.split(",");
          if (splitCoordinates.length == 2) {
            String longitude = splitCoordinates[0];
            String latitude = splitCoordinates[1];
            Double lng = Double.parseDouble(longitude.substring(1, longitude.length()));
            Double lat = Double.parseDouble(latitude.substring(0, latitude.length() - 1));
            obj.put("longitude", lng);
            obj.put("latitude", lat);
            //obj.put("radius", 4);
          }
        }
      } catch (Exception e) {
        //System.err.println("EEEEEEEERRRRRRROOOORRRRRR");
      }
      return obj;
    }

    private boolean putTweetInDatabase(String tweetText, Double lat, Double lng) throws IOException, JSONException {
      String url = "http://ws.geonames.org/countryCodeJSON?lat=" + lat + "&lng="
          + lng + "&username=goki";
      String countryObject = getCountryFromLocation(url);
      JSONObject obj = new JSONObject(countryObject);
      if (obj.has("countryName")) {
        String country = obj.getString("countryName");
        if (country != null) {
          try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection con = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/apache-flink-db?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&useSSL=false",
                "root", "");
            String query = " insert into country_tweet (country, tweet,latitude,longitude,radius)"
                + " values (?, ?, ?, ?, ?)";
            PreparedStatement preparedStmt = con.prepareStatement(query);
            preparedStmt.setString(1, country);
            preparedStmt.setString(2, tweetText);
            preparedStmt.setDouble(3, lat);
            preparedStmt.setDouble(4, lng);
            preparedStmt.setInt(5, 4);

            preparedStmt.execute();

            con.close();
            return true;
          } catch (Exception e) {

            System.out.println(e);
            return false;
          }
        }
      }
      return false;
    }

    private String getCountryFromLocation(String urlString) throws IOException {
      StringBuilder result = new StringBuilder();
      URL url = new URL(urlString);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      String line;
      while ((line = rd.readLine()) != null) {
        result.append(line);
      }
      rd.close();
      return result.toString();
    }
  }

  public static class WordCloudDataCreator implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
      if (jsonParser == null) {
        jsonParser = new ObjectMapper();
      }
      JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

      boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user")
          .get("lang")
          .asText()         //not only english tweets
          .equals("en");

      boolean hasText = jsonNode.has("text");
      if (isEnglish && hasText) {
        StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

        while (tokenizer.hasMoreTokens()) {
          String result = tokenizer.nextToken().trim().toLowerCase(); //.replaceAll("[^A-Za-z0-9#]", "")

          if (!result.equals("") && result.length() > 2) {
            out.collect(new Tuple2<>(result, 1));
          }
        }
      }
    }
  }
}


