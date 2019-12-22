package com.spark;

/**
 * 类名称：PointToDistance    
 * 类描述：两个百度经纬度坐标点，计算两点距离
 * 创建人：钟志铖    
 * 创建时间：2014-9-7 上午10:14:01    
 * 修改人：
 * 修改时间：
 * 修改备注：    
 * 版本信息：1.0  
 * 联系：QQ：433647
 */
public class PointToDistance {
 
	public static void main(String[] args) {
		getDistanceFromTwoPoints(23.5539530, 114.8903920, 23.5554550, 114.8868890);
		
		distanceOfTwoPoints(23.5539530, 114.8903920, 23.5554550, 114.8868890);
	}
	
	private static final Double PI = Math.PI;
 
	private static final Double PK = 180 / PI;
	
	/**
	 * @Description: 第一种方法
	 * @param lat_a
	 * @param lng_a
	 * @param lat_b
	 * @param lng_b
	 * @param @return   
	 * @return double
	 * @author 钟志铖
	 * @date 2014-9-7 上午10:11:35
	 */
	public static double getDistanceFromTwoPoints(double lat_a, double lng_a, double lat_b, double lng_b) {
		double t1 = Math.cos(lat_a / PK) * Math.cos(lng_a / PK) * Math.cos(lat_b / PK) * Math.cos(lng_b / PK);
		double t2 = Math.cos(lat_a / PK) * Math.sin(lng_a / PK) * Math.cos(lat_b / PK) * Math.sin(lng_b / PK);
		double t3 = Math.sin(lat_a / PK) * Math.sin(lat_b / PK);
 
		double tt = Math.acos(t1 + t2 + t3);
 
		System.out.println("两点间的距离：" + 6366000 * tt + " 米");
		return 6366000 * tt;
	}
 
	
	/********************************************************************************************************/
	// 地球半径
	private static final double EARTH_RADIUS = 6370996.81;
 
	// 弧度
	private static double radian(double d) {
		return d * Math.PI / 180.0;
	}
 
	/**
	 * @Description: 第二种方法
	 * @param lat1
	 * @param lng1
	 * @param lat2
	 * @param lng2   
	 * @return void
	 * @author 钟志铖
	 * @date 2014-9-7 上午10:11:55
	 */
	public static void distanceOfTwoPoints(double lat1, double lng1, double lat2, double lng2) {
		double radLat1 = radian(lat1);
		double radLat2 = radian(lat2);
		double a = radLat1 - radLat2;
		double b = radian(lng1) - radian(lng2);
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;
		s = Math.round(s * 10000) / 10000;
		double ss = s * 1.0936132983377;
		System.out.println("两点间的距离是：" + s + "米" + "," + (int) ss + "码");
	}
}