/**
 * Put your copyright and license info here.
 */
package com.example;

import java.util.HashMap;
import java.util.Map;

import benchmark.common.Utils;
import com.datatorrent.lib.io.IdempotentStorageManager;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.netlet.util.DTThrowable;

@ApplicationAnnotation(name = "Apex_Benchmark")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafka", new KafkaSinglePortStringInputOperator());
    DeserializeJSON deserializeJSON = dag.addOperator("deserialize", new DeserializeJSON());
    FilterTuplesAndFields filterTuplesAndFields = dag.addOperator("filterFields", new FilterTuplesAndFields() );
    RedisJoin redisJoin = dag.addOperator("redisJoin", new RedisJoin());
    CampaignProcessor campaignProcessor = dag.addOperator("output", new CampaignProcessor());

    // TODO : fix the path
    String configPath = new String();
    Map commonConfig = Utils.findAndReadConfigFile(configPath, true);
    // String zkServerHosts = joinHosts((List<String>)commonConfig.get("zookeeper.servers"),
    // Integer.toString((Integer)commonConfig.get("zookeeper.port")));

    String redisServerHost = (String)commonConfig.get("redis.host");

    kafkaInput.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());
    kafkaInput.setInitialPartitionCount(((Number)commonConfig.get("kafka.partitions")).intValue());
    kafkaInput.getConsumer().setTopic((String)commonConfig.get("kafka.topic"));
    kafkaInput.getConsumer().setZookeeper("zookeeper");
    kafkaInput.setInitialOffset("earliest");

    redisJoin.setRedisServerHost(redisServerHost);
    campaignProcessor.setRedisServerHost(redisServerHost);

    dag.addStream("kafka_deserialize", kafkaInput.outputPort, deserializeJSON.input);
    dag.addStream("deserialize_filterTuples", deserializeJSON.output, filterTuplesAndFields.input);
    dag.addStream("FilterFields_redisJoin", filterTuplesAndFields.output, redisJoin.input);
    dag.addStream("redisJoin_output", redisJoin.output, campaignProcessor.input);
  }

  @Stateless
  public class DeserializeJSON extends BaseOperator
  {
    public transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
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
  public class FilterTuplesAndFields extends BaseOperator
  {
    public transient DefaultInputPort<JSONObject> input = new DefaultInputPort<JSONObject>() {
      @Override
      public void process(JSONObject jsonObject)
      {
        try {
          if (  jsonObject.getString("event_type").equals("view") ) {
            Map<String, String> map = new HashMap<>();
            try {
              map.put("ad_id", jsonObject.getString("event_time") );
              map.put("event_time", jsonObject.getString("event_time") );
            } catch (JSONException e) {
              e.printStackTrace();
            }

            output.emit(map);
          }
        } catch (JSONException e) {
          DTThrowable.wrapIfChecked(e);
        }
      }
    };

    public transient DefaultOutputPort<Map<String,String>> output = new DefaultOutputPort();
  }
}
