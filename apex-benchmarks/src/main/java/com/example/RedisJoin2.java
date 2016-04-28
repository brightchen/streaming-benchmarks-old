package com.example;

import benchmark.common.advertising.RedisAdCampaignCache;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by sandesh on 3/18/16.
 */
public class RedisJoin2 extends BaseOperator
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

    public transient DefaultInputPort<Tuple2> input = new DefaultInputPort<Tuple2>()
    {
        @Override
        public void process(Tuple2 tuple)
        {
            String campaign_id =  redisAdCampaignCache.execute(tuple.adId);

            if (campaign_id == null || campaign_id.isEmpty()) {
                return;
            }

            tuple.campaignId = campaign_id;

            output.emit(tuple);
        }
    };

    public transient DefaultOutputPort<Tuple2> output = new DefaultOutputPort();

    @Override
    public void setup(Context.OperatorContext context)
    {
        this.redisAdCampaignCache = new RedisAdCampaignCache(redisServerHost);
        this.redisAdCampaignCache.prepare();
    }
}
