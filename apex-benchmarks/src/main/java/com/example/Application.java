/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package com.example;

import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.advertising.RedisAdCampaignCache;
import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.netlet.util.DTThrowable;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

@ApplicationAnnotation(name = "Apex_Benchmark")
public class Application implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Create operators for each step
    // settings are applied by the platform using the config file.
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput", new KafkaSinglePortStringInputOperator());
    DeserializeJSON deserializeJSON = dag.addOperator("deserialize", new DeserializeJSON());
    FilterTuples filterTuples = dag.addOperator("filterTuples", new FilterTuples() );
    FilterFields filterFields = dag.addOperator("filterFields", new FilterFields() );
    RedisJoin redisJoin = dag.addOperator("redisJoin", new RedisJoin());
    CampaignProcessor campaignProcessor = dag.addOperator("campaignProcessor", new CampaignProcessor());

    // kafkaInput.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());

    // Connect the Ports in the Operators
    dag.addStream("kafka_deserialize", kafkaInput.outputPort, deserializeJSON.input);
    dag.addStream("deserialize_filterTuples", deserializeJSON.output, filterTuples.input);
    dag.addStream("filterTuples_filterFields", filterTuples.output, filterFields.input);
    dag.addStream("FilterFields_redisJoin", filterFields.output, redisJoin.input);
    dag.addStream("campaignProcessor_output", redisJoin.output, campaignProcessor.input);
  }

  @Stateless
  public static class DeserializeJSON extends BaseOperator
  {
    public transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
      @Override
      public void process(String t)
      {
        JSONObject jsonObject;
        try {
          jsonObject = new JSONObject(t);
        } catch (JSONException e) {
          throw DTThrowable.wrapIfChecked(e);
        }

        output.emit(jsonObject);
      }
    };

    public transient DefaultOutputPort<JSONObject> output = new DefaultOutputPort();
  }

  @Stateless
  public static class FilterFields extends BaseOperator
  {
    public transient DefaultInputPort<JSONObject> input = new DefaultInputPort<JSONObject>()
    {
      @Override
      public void process(JSONObject jsonObject)
      {
        try {

          Map<String, String> map = new HashMap<>();

          map.put("ad_id", jsonObject.getString("ad_id") );
          map.put("event_time", jsonObject.getString("event_time") );

          output.emit(map);
        } catch (JSONException e) {
          DTThrowable.wrapIfChecked(e);
        }
      }
    };

    public transient DefaultOutputPort<Map<String,String>> output = new DefaultOutputPort();
  }

  @Stateless
  public static class FilterTuples extends BaseOperator
  {
    public transient DefaultInputPort<JSONObject> input = new DefaultInputPort<JSONObject>()
    {
      @Override
      public void process(JSONObject jsonObject)
      {
        try {
          if (  jsonObject.getString("event_type").equals("view") ) {
            output.emit(jsonObject);
          }
        } catch (JSONException e) {
          DTThrowable.wrapIfChecked(e);
        }
      }
    };

    public transient DefaultOutputPort<JSONObject> output = new DefaultOutputPort();
  }

  public static class RedisJoin extends BaseOperator
  {
    private RedisAdCampaignCache redisAdCampaignCache;
    private String redisServerHost;

    public String getRedisServerHost()
    {
      return redisServerHost;
    }

    public void setRedisServerHost(String redisServerHost)
    {
      this.redisServerHost = redisServerHost;
    }

    public transient DefaultInputPort<Map<String,String>> input = new DefaultInputPort<Map<String,String>>()
    {
      @Override
      public void process(Map<String,String> map)
      {
        String campaign_id = redisAdCampaignCache.execute(map.get("ad_id"));

        if (campaign_id == null || campaign_id.isEmpty()) {
          return;
        }

        map.put("campaign_id", campaign_id);

        output.emit(map);
      }
    };

    public transient DefaultOutputPort<Map<String,String>> output = new DefaultOutputPort();

    @Override
    public void setup(Context.OperatorContext context)
    {
      this.redisAdCampaignCache = new RedisAdCampaignCache(redisServerHost);
      this.redisAdCampaignCache.prepare();
    }
  }

  public static class CustomSerializer extends Serializer<CampaignProcessorCommon> {
    public void write (Kryo kryo, Output output, CampaignProcessorCommon object) {
      ObjectOutputStream out = null;
      try {
        out = new ObjectOutputStream(output);
        out.writeObject(object);
        out.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public CampaignProcessorCommon read (Kryo kryo, Input input, Class<CampaignProcessorCommon> type) {
      ObjectInputStream in = null;
      CampaignProcessorCommon e = null;

      try {
        in = new ObjectInputStream(input);
         e = (CampaignProcessorCommon) in.readObject();
        in.close();
      } catch (IOException ioException) {
        ioException.printStackTrace();
      } catch (ClassNotFoundException e1) {
        e1.printStackTrace();

      }

      return e;
    }
  }

  public static class CampaignProcessor extends BaseOperator
  {
    @FieldSerializer.Bind(CustomSerializer.class)
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

    public transient DefaultInputPort<Map<String,String>> input = new DefaultInputPort<Map<String,String>>()
    {
      @Override
      public void process(Map<String,String> map)
      {
        try {
          campaignProcessorCommon.execute(map.get("campaign_id"), map.get("auto_id"));
        }
        catch ( NumberFormatException exception ) {
            LOG.error(map.get("campaign_id") + " + " + map.get("auto_id") );
        }
      }
    };

    public void setup(Context.OperatorContext context)
    {
      campaignProcessorCommon = new CampaignProcessorCommon(redisServerHost);
      this.campaignProcessorCommon.prepare();
    }
  }

}
