package com.spark;

import com.alibaba.fastjson.JSON;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author ${张世林}
 * @date 2019/12/20
 * 作用：
 */
public class Json {

    public static void main(String[] args) throws IOException {
        List<Object> list = new ArrayList<Object>();
        for (int i = 0; i < 20000000; i++) {
//            Random random = new Random(i);
//            int i1 = random.nextInt();
            HashMap<String, Object> map = new HashMap<>();
            map.put("event_id", System.currentTimeMillis());
            map.put("city_id", 500000 + i);
            randomLonLat(85, 122, 29, 116, map);
            map.put("tel", RandomValue.getTel());
            list.add(JSON.toJSON(map));
        }

        list.forEach(System.out::println);
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File("json.json")));
        for (int i = 0; i < list.size(); i++) {
            bufferedWriter.write(list.get(i).toString());
            bufferedWriter.newLine();
        }
        bufferedWriter.close();

    }


    public static void randomLonLat(double MinLon, double MaxLon, double MinLat, double MaxLat, Map<String,Object> map) {
        BigDecimal db = new BigDecimal(Math.random() * (MaxLon - MinLon) + MinLon);
        String lon = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();// 小数后6位
        db = new BigDecimal(Math.random() * (MaxLat - MinLat) + MinLat);
        String lat = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();
        map.put("J", lon);
        map.put("W", lat);
    }

}
