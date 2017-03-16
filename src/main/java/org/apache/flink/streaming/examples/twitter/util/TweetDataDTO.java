package org.apache.flink.streaming.examples.twitter.util;

/**
 * Created by Grozdan.Madjarov on 3/14/2017.
 */
public class TweetDataDTO {
  public String name, latitude, longitude, radius;

  public TweetDataDTO(String name, String latitude, String longitude, String radius) {
    this.name = name;
    this.latitude = latitude;
    this.longitude = longitude;
    this.radius = radius;
  }
}
