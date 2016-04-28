package com.example;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * Created by sandesh on 3/18/16.
 */
public class DeserializeJSON extends BaseOperator
{
	JsonParser parser = new JsonParser();
    public transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
        @Override
        public void process(String t)
        {
            try {
                output.emit(parser.parse(t).getAsJsonObject());
            } catch (Exception e) {
                throw DTThrowable.wrapIfChecked(e);
            }

        }
    };

    public transient DefaultOutputPort<JsonObject> output = new DefaultOutputPort();
}
