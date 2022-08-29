package com.elta.taxi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("taxi");
        sparkConf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<TaxiOrder> taxiOrdersPersist = sc
                .textFile("data/taxi_order.txt")
                .map(TaxiOrder::convertFromLine)
                .persist(StorageLevel.MEMORY_AND_DISK());

        printLabResult(1, "The number of lines in the \"taxi_order\" file is " + taxiOrdersPersist.count());

        JavaRDD<TaxiOrder> boston = taxiOrdersPersist
                .filter(taxiOrder -> taxiOrder.getCity().equalsIgnoreCase("boston"));

        long numberOfLongDriveToBoston = boston
                .filter(taxiOrder -> taxiOrder.getDistance() >= 10)
                .count();
        printLabResult(2, "The number of long drives to Boston is " + numberOfLongDriveToBoston);

        double sumOfDistanceToBoston = boston.mapToDouble(TaxiOrder::getDistance).sum();

        printLabResult(3, "The sum of distances from all drives to Boston is " + sumOfDistanceToBoston);

        JavaPairRDD<Integer, String> drivers = sc
                .textFile("data/drivers.txt")
                .map(Driver::convertFromLine)
                .mapToPair(driver -> new Tuple2<>(driver.getId(), driver.getName()));

        List<Tuple2<Integer, String>> topDrivers = taxiOrdersPersist
                .mapToPair(taxiOrder -> new Tuple2<>(taxiOrder.getDriverId(), taxiOrder.getDistance()))
                .reduceByKey(Integer::sum)
                .join(drivers)
                .mapToPair(driverOrders -> new Tuple2<>(driverOrders._2._1, driverOrders._2._2))
                .sortByKey(false)
                .take(3);

        StringBuilder lab4Messages = new StringBuilder();
        lab4Messages.append("The top 3 drivers with the most total kilometers:\n");
        topDrivers.forEach(t1T2Tuple2 -> lab4Messages.append("\tName: " + t1T2Tuple2._2 + ", Total distance: " + t1T2Tuple2._1 + "\n"));
        printLabResult(4, lab4Messages.toString());

        sc.close();
    }

    private static void printLabResult(int labNumber, String message) {
        System.out.println("Lab " + labNumber + ":");
        System.out.println(message);
        System.out.println();
    }
}
