package com.atguigu.partitioner;

import com.atguigu.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean flowBean, int numPartitions) {
        //key:手机号  value: 流量信息
        String suffix = key.toString().substring(0, 3);
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
