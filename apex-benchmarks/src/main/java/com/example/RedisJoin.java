package com.example;

import benchmark.common.advertising.RedisAdCampaignCache;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import java.util.Map;

public class RedisJoin extends BaseOperator {

    private RedisAdCampaignCache redisAdCampaignCache;
    private String redisServerHost;

    public String getRedisServerHost() {
        return redisServerHost;
    }

    public void setRedisServerHost(String redisServerHost) {
        this.redisServerHost = redisServerHost;
    }

    public transient DefaultInputPort<Map<String,String>> input = new DefaultInputPort<Map<String,String>>() {
        @Override
        public void process(Map<String,String> map)
        {
            String campaign_id = redisAdCampaignCache.execute(map.get("ad_id"));

            if(campaign_id == null) {
                return;
            }

            map.put("campaign_id", campaign_id) ;

            output.emit(map);
        }
    };

    public transient DefaultOutputPort<Map<String,String>> output = new DefaultOutputPort();

    @Override
    public void setup(Context.OperatorContext context) {
        this.redisAdCampaignCache = new RedisAdCampaignCache(redisServerHost);
        this.redisAdCampaignCache.prepare();
    }
}

