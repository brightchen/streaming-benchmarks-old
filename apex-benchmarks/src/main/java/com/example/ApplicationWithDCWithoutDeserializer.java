package com.example;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;

@ApplicationAnnotation(name = ApplicationWithDCWithoutDeserializer.APP_NAME)
public class ApplicationWithDCWithoutDeserializer extends ApplicationDimensionComputation
{
  public static final String APP_NAME = "AppWithDCWithoutDe";
  
  protected static final int PARTITION_NUM = 8;
  
  public ApplicationWithDCWithoutDeserializer()
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
    JasonEventGenerator eventGenerator = dag.addOperator("eventGenerator", new JasonEventGenerator());
    FilterTuples filterTuples = dag.addOperator("filterTuples", new FilterTuples());
    FilterFields filterFields = dag.addOperator("filterFields", new FilterFields());
    RedisJoin redisJoin = dag.addOperator("redisJoin", new RedisJoin());

    setupRedis(eventGenerator.getCampaigns());
    
    // Connect the Ports in the Operators
    dag.addStream("filterTuples", eventGenerator.out, filterTuples.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filterFields", filterTuples.output, filterFields.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("redisJoin", filterFields.output, redisJoin.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    //dag.addStream("output", redisJoin.output, campaignProcessor.input);

    dag.setInputPortAttribute(filterTuples.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterFields.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(redisJoin.input, Context.PortContext.PARTITION_PARALLEL, true);

    dag.setAttribute(eventGenerator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<EventGenerator>(PARTITION_NUM));

    return redisJoin.output;
  }
  
  private void setupRedis(Map<String, List<String>> campaigns) {

    RedisHelper redisHelper = new RedisHelper();
    redisHelper.init("node35.morado.com");

    redisHelper.prepareRedis2(campaigns);
}
}
