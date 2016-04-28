package com.example;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class TupleGenerateOperator implements InputOperator
{
  public final transient DefaultOutputPort<Tuple> outputPort = new DefaultOutputPort<Tuple>();
  
  private int batchSize = 10;
  private int batchSleepTime = 2;
  private SimpleTupleGenerator tupleGenerator = new SimpleTupleGenerator();

  @Override
  public void emitTuples() {
    outputPort.emit(tupleGenerator.next());
  }
  

  public SimpleTupleGenerator getTupleGenerator()
  {
    return tupleGenerator;
  }

  public void setTupleGenerator(SimpleTupleGenerator tupleGenerator)
  {
    this.tupleGenerator = tupleGenerator;
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public int getBatchSleepTime()
  {
    return batchSleepTime;
  }

  public void setBatchSleepTime(int batchSleepTime)
  {
    this.batchSleepTime = batchSleepTime;
  }


  @Override
  public void beginWindow(long arg0)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext arg0)
  {
  }

  @Override
  public void teardown()
  {
  }
}
