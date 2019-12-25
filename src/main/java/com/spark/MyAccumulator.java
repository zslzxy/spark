package com.spark;

import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义累加器，累加器只能在Driver中才能使用
 */
public class MyAccumulator extends AccumulatorV2<ReduceData, List<ReduceData>> implements Serializable {
    private List<ReduceData> list = new ArrayList<>();
    @Override
    public boolean isZero() {
        return list.isEmpty();
    }

    @Override
    public AccumulatorV2<ReduceData, List<ReduceData>> copy() {
        return null;
    }

    @Override
    public void reset() {
        list.clear();
    }

    @Override
    public void add(ReduceData v) {
        list.add(v);
    }

    @Override
    public void merge(AccumulatorV2<ReduceData, List<ReduceData>> other) {

    }

    @Override
    public List<ReduceData> value() {
        return list;
    }
}