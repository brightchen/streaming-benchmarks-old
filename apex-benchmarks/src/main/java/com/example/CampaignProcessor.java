package com.example;

import java.util.Map;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import benchmark.common.advertising.CampaignProcessorCommon;

public class CampaignProcessor extends BaseOperator
{
  private CampaignProcessorCommon campaignProcessorCommon;
  private String redisServerHost;

  public String getRedisServerHost()
  {
    return redisServerHost;
  }

  public void setRedisServerHost(String redisServerHost)
  {
    this.redisServerHost = redisServerHost;
  }

  public transient DefaultInputPort<Map<String,String>> input = new DefaultInputPort<Map<String,String>>() {
    @Override
    public void process(Map<String,String> map)
    {
      campaignProcessorCommon.execute(map.get("campaign_id"), map.get("auto_id"));
    }
    };

  public void setup(Context.OperatorContext context)
  {
    campaignProcessorCommon = new CampaignProcessorCommon(redisServerHost);
    this.campaignProcessorCommon.prepare();
  }
}

