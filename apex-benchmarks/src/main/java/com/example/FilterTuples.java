package com.example;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Created by sandesh on 3/18/16.
 */
@Stateless
public class FilterTuples extends BaseOperator
{
    public transient DefaultInputPort<JSONObject> input = new DefaultInputPort<JSONObject>()
    {
        @Override
        public void process(JSONObject jsonObject)
        {
            try {
                if ( jsonObject.getString("event_type").equals("view") ) {
                    output.emit(jsonObject);
                }
            } catch (JSONException e) {
                DTThrowable.wrapIfChecked(e);
            }
        }
    };

    public transient DefaultOutputPort<JSONObject> output = new DefaultOutputPort();
}
