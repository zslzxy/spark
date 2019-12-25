//package com.spark;
//
//import org.apache.spark.util.AccumulatorV2;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class StrAccu extends AccumulatorV2<ReduceData> {
//
//    List<ReduceData> list = new ArrayList<ReduceData>();
//    @Override
//    public boolean isZero() {
//        return list.isEmpty();
//    }
//
//    @Override
//    public AccumulatorV2 copy() {
//        return null;
//    }
//
//    @Override
//    public void reset() {
//        list.clear();
//    }
//
//    @Override
//    public Object value() {
//        return list;
//    }
//
//    @Override
//    public void merge(AccumulatorV2 other) {
//
//    }
//
//    @Override
//    public void add(Object v) {
//        list.add(v);
//    }
//}