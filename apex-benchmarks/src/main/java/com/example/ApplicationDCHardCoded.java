package com.example;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import com.example.Tuple.TupleAggregator;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

@ApplicationAnnotation(name = ApplicationDCHardCoded.APP_NAME)
public class ApplicationDCHardCoded extends ApplicationDimensionComputation
{
  public static final String APP_NAME = "DCHardCoded";

  
  /**
   * this is used for hard coded tuple
   * @param dag
   * @param conf
   * @param upstreamPort
   */
  public void populateDimensionsDAG(DAG dag, Configuration conf, DefaultOutputPort<Tuple> upstreamPort) 
  {
    //Declare operators

    DimensionsComputation<Tuple, Tuple.TupleAggregateEvent> dimensions = new DimensionsComputation<>();
    dag.addOperator("DimensionsComputation", dimensions);
    DimensionsComputationUnifierImpl<Tuple, Tuple.TupleAggregateEvent> unifier = new DimensionsComputationUnifierImpl<>();
    dimensions.setUnifier(unifier);

    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 10);
    TupleConverter tupleConverter = dag.addOperator("TupleConverter", new TupleConverter());
    

    String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);
    //input.setEventSchemaJSON(eventSchema);

    String[] dimensionSpecs = new String[] {
      "time=" + TimeUnit.MINUTES,
      "time=" + TimeUnit.MINUTES + ":adId",
      "time=" + TimeUnit.MINUTES + ":campaignId",
      "time=" + TimeUnit.MINUTES + ":adId:campaignId"
    };

    //Set operator properties
    TupleAggregator[] aggregators = new TupleAggregator[dimensionSpecs.length];

    //Set input properties
    //input.setEventSchemaJSON(eventSchema);

    for(int index = 0;
        index < dimensionSpecs.length;
        index++) {
      String dimensionSpec = dimensionSpecs[index];
      TupleAggregator aggregator = new TupleAggregator();
      aggregator.init(dimensionSpec, index);
      aggregators[index] = aggregator;
    }

    unifier.setAggregators(aggregators);
    dimensions.setAggregators(aggregators);
    dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB, 8092);
    

    
    //Configuring the converter
    tupleConverter.setEventSchemaJSON(eventSchema);
    tupleConverter.setDimensionSpecs(dimensionSpecs);

    
    // store
    AppDataSingleSchemaDimensionStoreHDHT store = createStore(dag, conf, eventSchema); 
    
    PubSubWebSocketAppDataQuery query = createQuery(dag, conf, store);

    // wsOut
    PubSubWebSocketAppDataResult wsOut = createQueryResult(dag, conf, store);

    dag.setInputPortAttribute(dimensions.data, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(tupleConverter.inputPort, Context.PortContext.PARTITION_PARALLEL, true);

    dag.addStream("Generate", upstreamPort, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, tupleConverter.inputPort);
    dag.addStream("Converter", tupleConverter.outputPort, store.input);
    dag.addStream("QueryResult", store.queryResult, wsOut.input);
  }
  
}
