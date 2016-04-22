package com.example;


import java.util.Map;

import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.InputEvent;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema;

public class SpecificDimensionComputation extends AbstractDimensionsComputationFlexibleSingleSchema<Tuple>
{
  /**
   * This is a map from a key name (as defined in the
   * {@link com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema}) to the getter expression to use for
   * that key.
   */
  private Map<String, String> keyToExpression;
  /**
   * This is a map from a value name (as defined in the
   * {@link com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema}) to the getter expression to use for
   * that value.
   */
  private Map<String, String> aggregateToExpression;

  @Override
  public void convert(InputEvent inputEvent, Tuple tuple)
  {
    GPOMutable keys = inputEvent.getKeys();
    String[] stringFields = keys.getFieldsString();
    stringFields[0] = tuple.ad_id;
    stringFields[1] = tuple.campaign_id;
    
    keys.getFieldsLong()[0] = tuple.event_ime;
    inputEvent.getAggregates().getFieldsLong()[0] = tuple.clicks;
  }
  

  /**
   * @return the keyToExpression
   */
  public Map<String, String> getKeyToExpression()
  {
    return keyToExpression;
  }

  /**
   * @param keyToExpression the keyToExpression to set
   */
  public void setKeyToExpression(Map<String, String> keyToExpression)
  {
    this.keyToExpression = keyToExpression;
  }

  /**
   * @return the aggregateToExpression
   */
  public Map<String, String> getAggregateToExpression()
  {
    return aggregateToExpression;
  }

  /**
   * @param aggregateToExpression the aggregateToExpression to set
   */
  public void setAggregateToExpression(Map<String, String> aggregateToExpression)
  {
    this.aggregateToExpression = aggregateToExpression;
  }
  
}
