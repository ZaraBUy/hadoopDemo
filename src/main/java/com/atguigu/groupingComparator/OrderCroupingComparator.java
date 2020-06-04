package com.atguigu.groupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderCroupingComparator extends WritableComparator {

    protected OrderCroupingComparator(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean abean = (OrderBean) a;
        OrderBean bbean = (OrderBean) a;
        if(abean.getOrderId() > bbean.getOrderId()){
            return 1;
        }else if(abean.getOrderId() < bbean.getOrderId()){
            return -1;
        }else{
            return 0;
        }
    }
}
