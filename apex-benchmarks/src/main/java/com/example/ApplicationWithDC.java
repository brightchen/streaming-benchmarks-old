package com.example;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;

import java.util.List;
import java.util.Map;

@ApplicationAnnotation(name = ApplicationWithDC.APP_NAME)
public class ApplicationWithDC extends ApplicationDimensionComputation
{
  public static final String APP_NAME = "ApplicationWithDC";

  public ApplicationWithDC()
  {
    super(APP_NAME);
  }

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    DefaultOutputPort<Tuple> upstreamOutput = populateUpstreamDAG(dag, configuration);

    //populateHardCodedDimensionsDAG(dag, configuration, generateOperator.outputPort);
    populateDimensionsDAG(dag, configuration, upstreamOutput);
  }

  public DefaultOutputPort<Tuple> populateUpstreamDAG(DAG dag, Configuration configuration)
  {

    EventGenerator eventGenerator = dag.addOperator("eventGenerator", new EventGenerator());
    DeserializeJSON deserializeJSON = dag.addOperator("deserialize", new DeserializeJSON());
    FilterTuples filterTuples = dag.addOperator("filterTuples", new FilterTuples());
    FilterFields filterFields = dag.addOperator("filterFields", new FilterFields());
    RedisJoin redisJoin = dag.addOperator("redisJoin", new RedisJoin());
    setupRedis(eventGenerator.getCampaigns());
  //  CampaignProcessor campaignProcessor = dag.addOperator("campaignProcessor", new CampaignProcessor());


    // Connect the Ports in the Operators
    dag.addStream("deserialize", eventGenerator.out, deserializeJSON.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filterTuples", deserializeJSON.output, filterTuples.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filterFields", filterTuples.output, filterFields.input).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.addStream("redisJoin", filterFields.output, redisJoin.input).setLocality(DAG.Locality.THREAD_LOCAL);
    //dag.addStream("output", redisJoin.output, campaignProcessor.input);

    dag.setInputPortAttribute(deserializeJSON.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterTuples.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterFields.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(redisJoin.input, Context.PortContext.PARTITION_PARALLEL, true);

    dag.setAttribute(eventGenerator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<EventGenerator>(1));

    return redisJoin.output;
  }

  private void setupRedis(Map<Integer, List<Integer>> campaigns) {

    RedisHelper redisHelper = new RedisHelper();
    redisHelper.init("node35.morado.com");

    redisHelper.prepareRedis(campaigns);
  }
}
