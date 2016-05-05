package com.example;

import java.util.Random;

public class SimpleTupleGenerator
{
  public static final String[] adIds = {"ad1", "ad2", "ad3", "ad4", "ad5", "ad6", "ad7", "ad8", "ad9", "ad10" };
  public static final String[] campaignIds = {"campaign1", "campaign2", "campaign3", "campaign4", "campaign5", "campaign6", "campaign7", "campaign8", "campaign9", "campaign10" };
  public static final Long[] eventTimeShifts = {-25000L, -15000L, 0L};
  public static final int maxClicks = 1000;
  
  protected static final Random random = new Random();
  public Tuple next()
  {
    return new Tuple(randomValue(adIds), randomValue(campaignIds), System.currentTimeMillis() + randomValue(eventTimeShifts), random.nextInt(maxClicks));
  }
  
  public <T> T randomValue(T[] array)
  {
    return array[random.nextInt(array.length)];
  }
}
