package com.example;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class JasonEventGenerator extends BaseOperator implements InputOperator
{
  protected List<String> ad_id;
  private static String pageID = UUID.randomUUID().toString();
  private static String userID = UUID.randomUUID().toString();
  private static final String[] eventTypes = new String[] { "view", "click", "purchase" };
  private String mappingFile = "/user/sandesh/test2.txt";

  private final Map<String, List<String>> campaigns;
  
  public JasonEventGenerator()
  {
    this.campaigns = generateCampaigns();
    this.ad_id = flattenCampaigns();
  }
  
  public String getMappingFile()
  {
    return mappingFile;
  }

  public void setMappingFile(String mappingFile)
  {
    this.mappingFile = mappingFile;
  }

  public final transient DefaultOutputPort<JSONObject> out = new DefaultOutputPort<JSONObject>();

  public void setup(Context.OperatorContext context)
  {
  }

  public JSONObject generateElement() throws JSONException
  {
    JSONObject obj = new JSONObject();
    obj.put("user_id", userID);
    obj.put("page_id", pageID);
    
    obj.put("ad_id", ad_id.get(ThreadLocalRandom.current().nextInt(ad_id.size())));
    obj.put("ad_type", "banner78");
    obj.put("event_type", eventTypes[ThreadLocalRandom.current().nextInt(eventTypes.length)]);
    obj.put("event_time", System.currentTimeMillis());
    obj.put("ip_address", "1.2.3.4");
    
    return obj;
  }

  @Override
  public void emitTuples()
  {
    for(int i=0; i<100; ++i)
    {
      try
      {
        out.emit(generateElement());
      }
      catch(Exception e)
      {
        //ignore
      }
    }
      
  }

  public Map<String, List<String>> getCampaigns()
  {
    return campaigns;
  }

  /**
   * Generate a random list of ads and campaigns
   */
  private Map<String, List<String>> generateCampaigns()
  {
    int numCampaigns = 100;
    int numAdsPerCampaign = 10;
    Map<String, List<String>> adsByCampaign = new LinkedHashMap<>();
    for (int i = 0; i < numCampaigns; i++) {
      String campaign = UUID.randomUUID().toString();
      ArrayList<String> ads = new ArrayList<>();
      adsByCampaign.put(campaign, ads);
      for (int j = 0; j < numAdsPerCampaign; j++) {
        ads.add(UUID.randomUUID().toString());
      }
    }
    
    return adsByCampaign;
  }

  /**
   * Flatten into just ads
   */
  private List<String> flattenCampaigns()
  {
    // Flatten campaigns into simple list of ads
    List<String> ads = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
      for (String ad : entry.getValue()) {
        ads.add(ad);
      }
    }
    return ads;
  }
}

