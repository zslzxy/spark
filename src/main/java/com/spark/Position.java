package com.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author ${张世林}
 * @date 2019/12/22
 * 作用：
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Position implements Serializable {

    private long event_id;
    private long city_id;
    private double J;
    private double W;
    private String tel;

}
