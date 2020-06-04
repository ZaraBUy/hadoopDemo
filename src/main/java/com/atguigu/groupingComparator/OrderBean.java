package com.atguigu.groupingComparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;
    private double price;

    public OrderBean() {
    }

    public OrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    @Override
    public int compareTo(OrderBean bean) {
        if (orderId > bean.getOrderId()) {
            return 1;
        } else if (orderId < bean.getOrderId()) {
            return -1;
        } else {
            if (price > bean.getPrice()) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readInt();
        price = in.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
