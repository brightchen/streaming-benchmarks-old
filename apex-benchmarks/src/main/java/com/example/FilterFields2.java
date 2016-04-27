package com.example;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

@Stateless
public class FilterFields2 extends BaseOperator
{
    public transient DefaultInputPort<JSONObject> input = new DefaultInputPort<JSONObject>()
    {
        @Override
        public void process(JSONObject jsonObject)
        {
            try {

                Tuple2 tuple = new Tuple2();

                tuple.adId = jsonObject.getString("ad_id");
                tuple.event_time = jsonObject.getString("event_time");

                output.emit(tuple);
            } catch (JSONException e) {
            }
        }
    };

    public transient DefaultOutputPort<Tuple2> output = new DefaultOutputPort();
}
