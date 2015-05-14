/**
 * 
 */
package com.geospatial.operation4;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.geospatial.util.SystemConfiguration;

import scala.Tuple2;

/**
 * @author team15
 *
 */
public class ClosestPair {

    @SuppressWarnings({ "resource", "serial", "static-access" })
    public static void main(String args[]) throws IOException {

        // Read pom.xml
        SystemConfiguration sysConf = new SystemConfiguration();
        Properties p = sysConf.loadSystemProperties();

        // create the hdfs url
        URI uri = URI.create(p.getProperty("HDFS_URL"));
        Path hdfs = new Path(uri);

        // create a filesystem instance
        FileSystem fs = FileSystem.get(uri, new Configuration());

        // input file path on hdfs
        String inputFile = p.getProperty("closestpairInputFile");

        // set spark application name
        SparkConf conf = new SparkConf().setAppName("ClosestPairApplication").set("spark.executor.memory", "1830m");

        // set spark master url and memory per node value. Here 256MB is
        // allocated per node.
        // currently running on a cluster. change it to "local" to run on a
        // single node.
        conf.setMaster("spark://master:7077");
        // .set("spark.executor.memory",
        // p.getProperty("memory_per_node"));
        // conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // add the application jar file path to the spark context
        sc.addJar(p.getProperty("application_jar"));
        // add guava.jar dependency file path to the spark context
        sc.addJar(p.getProperty("guava_jar"));

        // Create an RDD of the input text file. Cache it for faster access.
        JavaRDD<String> inputRDD = sc.textFile(inputFile).cache();

        // Create an RDD of the input which holds the file contents line by line
        JavaRDD<String> input = inputRDD.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            public Iterable<String> call(String s) {
                return Arrays.asList(s.split("\n"));
            }
        });
        
     // Create an RDD of the input by filtering any of the points that cannot
        // be converted to double
        JavaRDD<String> ipFilteredRDDLineByLine = input.filter(new Function<String, Boolean>() {

            @SuppressWarnings("unused")
            public Boolean call(String s) {
                // split each string by ,
                String[] helper = s.split(",");
                Double x, y;
                // small test case
                if (helper.length == 2) {
                    try {
                        x = Double.parseDouble(helper[0]);
                        y = Double.parseDouble(helper[1]);
                        return true;
                    } catch (Exception ex) {
                        System.out.println(ex);
                        return false;
                    }
                } else {
                    try {
                        x = Double.parseDouble(helper[2]);
                        y = Double.parseDouble(helper[3]);
                        return true;
                    } catch (Exception ex) {
                        System.out.println(ex);
                        return false;
                    }
                }
            }

        });

     // Create an RDD of points
        JavaRDD<Point> points = ipFilteredRDDLineByLine.flatMap(new FlatMapFunction<String, Point>() {
            public Iterable<Point> call(String s) {
                String[] helper = s.split(",");
                Double x, y;
                Point p = new Point();
                if (helper.length == 2) {
                    x = Double.parseDouble(helper[0]);
                    y = Double.parseDouble(helper[1]);
                    p.setX(x);
                    p.setY(y);
                } else {
                    x = Double.parseDouble(helper[2]);
                    y = Double.parseDouble(helper[3]);
                    p.setX(x);
                    p.setY(y);
                }
                // create an iterable collection of points
                return Arrays.asList(p);
            }
        });

        // Create a Pair RDD of the input which holds all the points as Value V
        // with their X-coordinates as Key K
        JavaPairRDD<Double, Point> pointPair = points.flatMapToPair(new PairFlatMapFunction<Point, Double, Point>() {

            private static final long serialVersionUID = 1L;

            @SuppressWarnings("unchecked")
            public Iterable<Tuple2<Double, Point>> call(Point p) throws Exception {

                Tuple2<Double, Point> tuple = new Tuple2<Double, Point>(p.getX(), p);
                return Arrays.asList(tuple);
            }

        });

        // Create a sorted Pair RDD by X-axis xo-ordinates
        JavaPairRDD<Double, Point> sortedPointpair = pointPair.sortByKey();

        // create a list of points(after collecting candidate points from all
        // partition) which can be potential candidates for closest pair
        List<Point> candidatePoints = sortedPointpair.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Double, Point>>, Point>() {

            public Iterable<Point> call(Iterator<Tuple2<Double, Point>> t) throws Exception {

                List<Point> pointList = new ArrayList<Point>();
                List<Point> candPointList = new ArrayList<Point>();

                while (t.hasNext()) {
                    pointList.add(t.next()._2);
                }

                int len = pointList.size();

                Pair p = Pair.divideAndConquer(pointList);

                candPointList.add(p.point1);
                candPointList.add(p.point2);

                // min. distance in this partition

                double tempMin = Point.getDifference(p.point1, p.point2);
                // add candidate points within delta(tempMin)
                // distance

                Point p1 = pointList.get(0); // point with min.
                                             // X-co-ordinate

                if (p1 != null) {

                    double x_min = p1.x;

                    int i = 1;
                    // adding candidate points from left side
                    while (i<pointList.size()) {
                        Point tempPoint = pointList.get(i);
                        if (tempPoint != null && (tempPoint.x - x_min) < tempMin) {
                            candPointList.add(tempPoint);
                            i++;
                        } else {
                            break;
                        }
                    }

                }

                p1 = pointList.get(len - 1); // point with max.
                                             // X-co-ordinate

                if (p1 != null) {

                    double x_max = p1.x;

                    int i = len - 2;
                    // adding candidate points from left side
                    while (i>=0) {
                        Point tempPoint = pointList.get(i);
                        if (tempPoint != null && (x_max - tempPoint.x) < tempMin) {
                            candPointList.add(tempPoint);
                            i--;
                        } else {
                            break;
                        }
                    }

                }

                return candPointList;
            }

        }).repartition(1).collect();

        // final pair points
        Pair finalPair = Pair.divideAndConquer(candidatePoints);

        List<Point> closestPairList = new ArrayList<Point>();
        closestPairList.add(finalPair.point1);
        closestPairList.add(finalPair.point2);

        // delete the result folder on HDFS if it already exists.
        if (fs.exists(hdfs.mergePaths(hdfs, new Path(p.getProperty("closest_pair_result")))))
            fs.delete(hdfs.mergePaths(hdfs, new Path(p.getProperty("closest_pair_result"))), true);

        // save the closestpair points to a text file
        sc.parallelize(closestPairList).saveAsTextFile(p.getProperty("HDFS_URL") + p.getProperty("closest_pair_result"));

        System.out.println("closest pair points:: \n" + finalPair.point1 + "\n" + finalPair.point2);

    }
}
