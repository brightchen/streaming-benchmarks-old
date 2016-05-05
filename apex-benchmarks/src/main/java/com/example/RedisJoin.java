package com.example;

import benchmark.common.advertising.RedisAdCampaignCache;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by sandesh on 3/18/16.
 */
public class RedisJoin extends BaseOperator
{
    private transient RedisAdCampaignCache redisAdCampaignCache;
    private String redisServerHost;

    public String getRedisServerHost()
    {
        return redisServerHost;
    }

    public void setRedisServerHost(String redisServerHost)
    {
        this.redisServerHost = redisServerHost;
    }

    public transient DefaultInputPort<Tuple> input = new DefaultInputPort<Tuple>()
    {
        @Override
        public void process(Tuple tuple)
        {
          tuple.campaign_id = "";
            String campaign_id = redisAdCampaignCache.execute(tuple.ad_id);

            if (campaign_id == null || campaign_id.isEmpty()) {
                return;
            }

            tuple.campaign_id = campaign_id ;

            output.emit(tuple);
        }
    };

    public transient DefaultOutputPort<Tuple> output = new DefaultOutputPort();

    @Override
    public void setup(Context.OperatorContext context)
    {
        this.redisAdCampaignCache = new RedisAdCampaignCache(redisServerHost);
        this.redisAdCampaignCache.prepare();
    }
}
