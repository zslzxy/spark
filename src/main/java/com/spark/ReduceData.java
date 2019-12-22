package com.spark;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ${张世林}
 * @date 2019/12/22
 * 作用：
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ReduceData {
    private double lng;
    private double lng1;
    private double lat;
    private double lat1;
    private double length;
    private String type;
    private String center_val;
    private String relative_val;
    
}
