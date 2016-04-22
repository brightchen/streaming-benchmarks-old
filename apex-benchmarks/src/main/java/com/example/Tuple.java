package com.example;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent.Aggregator;

import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.statistics.DimensionsComputation;

/**
 * Created by sandesh on 3/18/16.
 */
public class Tuple implements Serializable
{
  private static final long serialVersionUID = -6258684464861711025L;

  public static final String ADID = "adId";
  public static final String CAMPAIGNID = "campaignId";
  public static final String EVENTTIME = "eventTime";
  public static final String CLICKS = "clicks";

  public String ad_id;
  public String campaign_id;
  public Long event_ime;
  public long clicks = 0;

  public Tuple()
  {
  }

  public Tuple(String adId, String campaignId, long eventTime, long clicks)
  {
    this.ad_id = adId;
    this.campaign_id = campaignId;
    this.event_ime = eventTime;
    this.clicks = clicks;
  }

  public String getAdId()
  {
    return ad_id;
  }

  public void setAdId(String adId)
  {
    this.ad_id = adId;
  }

  public String getCampaignId()
  {
    return campaign_id;
  }

  public void setCampaignId(String campaignId)
  {
    this.campaign_id = campaignId;
  }

  public long getEventTime()
  {
    return (event_ime == null) ? 0 : event_ime;
  }

  public void setEventTime(long eventTime)
  {
    this.event_ime = eventTime;
  }

  public long getTime()
  {
    return getEventTime();
  }

  public long getClicks()
  {
    return clicks;
  }

  public void setClicks(long clicks)
  {
    this.clicks = clicks;
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 71 * hash + this.ad_id.hashCode();
    hash = 71 * hash + this.campaign_id.hashCode();
    hash = 71 * hash + (int)(long)this.event_ime;
    hash = 71 * hash + (int)this.clicks;

    return hash;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || !(o instanceof Tuple)) {
      return false;
    }

    Tuple tuple = (Tuple)o;

    return this.ad_id.equals(tuple.ad_id) && this.campaign_id.equals(tuple.campaign_id) && this.event_ime == tuple.event_ime
        && this.clicks == tuple.clicks;
  }
  
  public static class TupleAggregateEvent extends Tuple implements AggregateEvent
  {
    private static final long serialVersionUID = 1L;
    int aggregatorIndex;
    public int timeBucket;
    private int dimensionsDescriptorID;

    public TupleAggregateEvent()
    {
      //Used for kryo serialization
    }

    public TupleAggregateEvent(int aggregatorIndex)
    {
      this.aggregatorIndex = aggregatorIndex;
    }

    @Override
    public int getAggregatorIndex()
    {
      return aggregatorIndex;
    }

    public void setAggregatorIndex(int aggregatorIndex)
    {
      this.aggregatorIndex = aggregatorIndex;
    }

    /**
     * @return the dimensionsDescriptorID
     */
    public int getDimensionsDescriptorID()
    {
      return dimensionsDescriptorID;
    }

    /**
     * @param dimensionsDescriptorID
     *          the dimensionsDescriptorID to set
     */
    public void setDimensionsDescriptorID(int dimensionsDescriptorID)
    {
      this.dimensionsDescriptorID = dimensionsDescriptorID;
    }

    @Override
    public int hashCode()
    {
      int hash = 5;
      hash = 71 * hash + this.ad_id.hashCode();
      hash = 71 * hash + this.campaign_id.hashCode();
      hash = 71 * hash + (int)(long)this.event_ime;
      hash = 71 * hash + (int)this.clicks;
      hash = 71 * hash + this.timeBucket;

      return hash;
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == null || !(o instanceof TupleAggregateEvent)) {
        return false;
      }

      TupleAggregateEvent aae = (TupleAggregateEvent)o;

      return this.ad_id.equals(aae.ad_id) && this.campaign_id.equals(aae.campaign_id) && this.event_ime == aae.event_ime
          && this.clicks == aae.clicks && this.timeBucket == aae.timeBucket;
    }
  }

  
  public static class TupleAggregator implements Aggregator<Tuple, TupleAggregateEvent>
  {
    String dimension;
    TimeBucket timeBucket;
    int timeBucketInt;
    TimeUnit time;
    
    boolean hasAdId;
    boolean hasCompainId;
    int dimensionsDescriptorID;

    public void init(String dimension, int dimensionsDescriptorID)
    {
      String[] attributes = dimension.split(":");
      for (String attribute : attributes) {
        String[] keyval = attribute.split("=", 2);
        String key = keyval[0];
        if (key.equals("time")) {
          time = TimeUnit.valueOf(keyval[1]);
          timeBucket = TimeBucket.TIME_UNIT_TO_TIME_BUCKET.get(time);
          timeBucketInt = timeBucket.ordinal();
          time = timeBucket.getTimeUnit();
        }
        else if (key.equals("adId")) {
          hasAdId = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
        }
        else if (key.equals("campaignId")) {
          hasCompainId = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
        }

        else {
          throw new IllegalArgumentException("Unknown attribute '" + attribute + "' specified as part of dimension!");
        }
      }

      this.dimensionsDescriptorID = dimensionsDescriptorID;
      this.dimension = dimension;
    }

    /**
     * Dimension specification for display in operator properties.
     * @return The dimension.
     */
    public String getDimension()
    {
      return dimension;
    }

    @Override
    public String toString()
    {
      return dimension;
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 37 * hash + (this.time != null ? this.time.hashCode() : 0);
      hash = 37 * hash + (this.hasAdId ? 1 : 0);
      hash = 37 * hash + (this.hasCompainId ? 1 : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final TupleAggregator other = (TupleAggregator) obj;
      if (this.time != other.time) {
        return false;
      }
      if (this.hasAdId != other.hasAdId) {
        return false;
      }
      if (this.hasCompainId != other.hasCompainId) {
        return false;
      }
      return true;
    }

    @Override
    public TupleAggregateEvent getGroup(Tuple src, int aggregatorIndex)
    {
      TupleAggregateEvent event = new TupleAggregateEvent(aggregatorIndex);
      event.event_ime = timeBucket.roundDown(src.event_ime);
      event.timeBucket = timeBucketInt;

      event.ad_id = src.ad_id;
      event.campaign_id = src.campaign_id;

      event.aggregatorIndex = aggregatorIndex;
      event.dimensionsDescriptorID = dimensionsDescriptorID;

      return event;
    }

    @Override
    public void aggregate(TupleAggregateEvent dest, Tuple src)
    {
      dest.clicks += src.clicks;
    }

    @Override
    public void aggregate(TupleAggregateEvent dest, TupleAggregateEvent src)
    {
      dest.clicks += src.clicks;
    }

    //@Override
    public int computeHashCode(Tuple event)
    {
      int hash = 5;
      hash = 71 * hash + event.ad_id.hashCode();
      hash = 71 * hash + event.campaign_id.hashCode();
      
      long ltime = time.convert(event.event_ime, TimeUnit.MILLISECONDS);
      hash = 71 * hash + (int) (ltime ^ (ltime >>> 32));
          
      return hash;
    }

    @Override
    public boolean equals(Tuple event1, Tuple event2)
    {
      if (event1 == event2) {
        return true;
      }

      if (event2 == null) {
        return false;
      }

      if (event1.getClass() != event2.getClass()) {
        return false;
      }

      if (!event1.ad_id.equals(event2.ad_id)) {
        return false;
      }
      
      if (!event1.campaign_id.equals(event2.campaign_id)) {
        return false;
      }
      
      if (time != null && time.convert(event1.event_ime, TimeUnit.MILLISECONDS) != time.convert(event2.event_ime, TimeUnit.MILLISECONDS)) {
        return false;
      }
      return true;
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201402211829L;

    @Override
    public int hashCode(Tuple o)
    {
      return computeHashCode(o);
    }
  }

}
