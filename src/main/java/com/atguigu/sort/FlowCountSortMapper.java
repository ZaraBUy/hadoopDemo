package com.atguigu.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text, PhoneFlowBean, Text> {

    PhoneFlowBean k = new PhoneFlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //13470253144	180	180	360
        String line = value.toString();
        String[] fields = line.split("\t");

        v.set(fields[0]);

        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);

        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(sumFlow);

        //在缓冲区中对key进行的排序,  所以要把flowbean写为Key
        context.write(k,v);
    }
}
