/**
 * Put your copyright and license info here.
 */
package com.datatorrent.benchmarks;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //
    // KafkaInputOperator kafkaInputOperator

    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    // rand.setTuplesBlast(1) ;
    // rand.setTuplesBlastIntervalMillis(1000);


  }
}
