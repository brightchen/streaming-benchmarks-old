package com.example;

import java.util.Calendar;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Context;

public class JsonTester
{
  @Test
  public void test() throws JSONException
  {
    TestJasonEventGenerator generator = new TestJasonEventGenerator();
    generator.setup(null);
    
    
    int count = 0;
    for(int i=0; i<100; ++i)
    {
      JSONObject jsonObject = generator.generateElement();
      long eventTime = Long.parseLong(jsonObject.getString("event_time"));
      Calendar c = Calendar.getInstance();
      c.setTimeInMillis(eventTime);
      String s = c.toString();
      
      count += (jsonObject.getString("event_type").equals("view") ? 1 : 0);
    }
    
    Assert.assertTrue("count is: " + count, count > 10);
  }
  
  
  public static class TestJasonEventGenerator extends JasonEventGenerator
  {
    public void setup(Context.OperatorContext context)
    {
      ad_id.add("1");
      ad_id.add("2");
    }
  }
}
