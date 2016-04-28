package com.example;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.google.gson.JsonObject;

@Stateless
public class FilterFields2 extends BaseOperator
{
    public transient DefaultInputPort<JsonObject> input = new DefaultInputPort<JsonObject>()
    {
        @Override
        public void process(JsonObject jsonObject)
        {
            try {

                Tuple2 tuple = new Tuple2();

                tuple.adId = jsonObject.get("ad_id").getAsString();
                tuple.event_time = jsonObject.get("event_time").getAsString();

                output.emit(tuple);
            } catch (Exception e) {
            }
        }
    };

    public transient DefaultOutputPort<Tuple2> output = new DefaultOutputPort();
}
