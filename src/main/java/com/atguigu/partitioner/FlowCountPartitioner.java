package com.atguigu.partitioner;

import com.atguigu.sort.PhoneFlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowCountPartitioner extends Partitioner<PhoneFlowBean, Text> {
    @Override
    public int getPartition(PhoneFlowBean phoneFlowBean, Text text, int numPartitions) {
        String suffix = text.toString().substring(0, 3);
        if ("136".equals(suffix)) {
            return 0;
        } else if ("137".equals(suffix)) {
            return 1;
        } else if ("138".equals(suffix)) {
            return 2;
        } else if ("139".equals(suffix)) {
            return 3;
        }
        return 4;
    }
}
