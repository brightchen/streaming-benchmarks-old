package com.example;

import java.io.Serializable;

/**
 * Created by sandesh on 3/18/16.
 */
public class Tuple2 implements Serializable
{
  public String adId;
  public String campaignId;

  public String event_time;
  public String clicks ;

  public Tuple2()
  {
  }

  public Tuple2(String adId, String campaignId, String eventTime, String clicks)
  {
    this.adId = adId;
    this.campaignId = campaignId;
    this.event_time = eventTime;
    this.clicks = clicks;
  }
}
