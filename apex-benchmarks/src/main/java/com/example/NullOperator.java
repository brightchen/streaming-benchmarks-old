package com.example;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by sandesh on 4/27/16.
 */
public class NullOperator extends BaseOperator {

    public transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
        @Override
        public void process(Object tuple)
        {

        }
    };
}
