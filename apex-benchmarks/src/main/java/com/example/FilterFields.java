package com.example;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.google.gson.JsonObject;

@Stateless
public class FilterFields extends BaseOperator
{
    public transient DefaultInputPort<JsonObject> input = new DefaultInputPort<JsonObject>()
    {
        @Override
        public void process(JsonObject jsonObject)
        {
            try {

                Tuple tuple = new Tuple();

                tuple.adId = jsonObject.get("ad_id").getAsLong();
                tuple.event_time = jsonObject.get("event_time").getAsLong();

                output.emit(tuple);
            } catch (Exception e) {
                throw new RuntimeException(e) ;
            }
        }
    };

    public transient DefaultOutputPort<Tuple> output = new DefaultOutputPort();
}
