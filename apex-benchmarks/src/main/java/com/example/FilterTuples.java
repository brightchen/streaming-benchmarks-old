package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;
import com.google.gson.JsonObject;

/**
 * Created by sandesh on 3/18/16.
 */
@Stateless
public class FilterTuples extends BaseOperator
{
    private static final Logger LOG = LoggerFactory.getLogger(FilterTuples.class);

    public transient DefaultInputPort<JsonObject> input = new DefaultInputPort<JsonObject>()
    {
        @Override
        public void process(JsonObject jsonObject)
        {
            try {
                if (  jsonObject.get("event_type").getAsString().equals("view") ) {
                    output.emit(jsonObject);
                }
            } catch (Exception e) {
                DTThrowable.wrapIfChecked(e);
            }
        }
    };

    public transient DefaultOutputPort<JsonObject> output = new DefaultOutputPort();
}
