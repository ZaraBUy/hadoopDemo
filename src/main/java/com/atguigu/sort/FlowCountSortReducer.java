package com.atguigu.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountSortReducer extends Reducer<PhoneFlowBean, Text, Text, PhoneFlowBean> {

    @Override
    protected void reduce(PhoneFlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value, key);
        }
    }
}
