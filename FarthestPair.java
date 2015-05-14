package com.geospatial.operation3;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.geospatial.operation2.Point;
import com.geospatial.util.SystemConfiguration;

/**
 * 
 * @author team15
 *
 */

public class FarthestPair {

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
        String inputFile = p.getProperty("farthestpairInputFile");

        // set spark application name
        SparkConf conf = new SparkConf().setAppName("FarthestPairApplication").set("spark.executor.memory", "1830m");

        // set spark master url and memory per node value. Here 256MB is
        // allocated per node.
        // currently running on a cluster. change it to "local" to run on a
        // single node.
        conf.setMaster("spark://master:7077");
        // conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // add the application jar file path to the spark context
        sc.addJar(p.getProperty("application_jar"));
        // add guava.jar dependency file path to the spark context
        sc.addJar(p.getProperty("guava_jar"));

        // Create an RDD of the input text file. Cache it for faster access.
        JavaRDD<String> inputRDD = sc.textFile(inputFile).cache();

        // Create an RDD of the input which holds the file contents line by line
        JavaRDD<String> ipRDDLineByLine = inputRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                return Arrays.asList(s.split("\n"));
            }
        });

        // Create an RDD of the input by filtering any of the points that cannot
        // be converted to double
        JavaRDD<String> ipFilteredRDDLineByLine = ipRDDLineByLine.filter(new Function<String, Boolean>() {

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
        
        // helper point to sort
        Point helper = new Point();

        // pivot point
        final Point min = points.min(helper);

        // store the pivot point
        Point.min = min;

        // sort by polar angle
        JavaRDD<Point> sortedPoints = points.distinct().sortBy(new Function<Point, Point>() {
            public Point call(Point p1) {
                return p1;
            }
        }, true, 1);

        // apply graham scan algorithm in a distributed way to calculate the
        // convex hull.
        // each partition returns a list of points that are most likely to be in
        // the convex hull.
        JavaRDD<Point> partHulls = sortedPoints.mapPartitions(new FlatMapFunction<Iterator<Point>, Point>() {

            public Iterable<Point> call(Iterator<Point> itr) {

                java.util.List<Point> data = new java.util.ArrayList<Point>();

                while (itr.hasNext()) {
                    data.add(itr.next());
                }

                for (int i = 0; i < data.size() - 2; i++) {

                    double angle = ccw(data.get(i), data.get(i + 1), data.get(i + 2));

                    if (angle < 0) {
                        // this point is not part of the convex hull.
                        data.remove(i + 1);
                        if ((i - 2) >= -1)
                            i = i - 2;
                        else
                            i = -1;
                    }
                }
                return data;
            }

            // calculate the direction in which the lines joining these points
            // is heading to.
            // 0 - collinear
            // > 0 - clockwise
            // < 0 - counter-clockwise
            public double ccw(Point a, Point b, Point c) {
                double x1, y1, x2, y2, x3, y3;
                x1 = a.getX();
                y1 = a.getY();

                x2 = b.getX();
                y2 = b.getY();

                x3 = c.getX();
                y3 = c.getY();

                return (x2 - x1) * (y3 - y1) - (y2 - y1) * (x3 - x1);
            }

        });

        // get all points in one partition
        JavaRDD<Point> result = partHulls.repartition(1);

        // convert to a list
        List<Point> listResult = result.collect();

        // sort once again according to polar angle
        Collections.sort(listResult);

        // apply graham scan algorithm once again, this time on one partition
        // that gives the co-ordinates of the convex hull
        for (int i = 0; i < listResult.size() - 2; i++) {

            double angle = ccw(listResult.get(i), listResult.get(i + 1), listResult.get(i + 2));
            if (angle < 0) {
                // this point is not part of the convex hull.
                listResult.remove(i + 1);
                if ((i - 2) >= -1)
                    i = i - 2;
                else
                    i = -1;
            }
        }

        // Perform brute-force approach to find max dist. between two points on
        // all candidate points,
        // i.e., points on Convex hull

        int len = listResult.size();
        double maxDiff = 0; // max distance between 2 points ..initializing with
                            // 0 value
        Point[] maxDiffPoints = new Point[2]; // will hold 2 temporary max.
                                              // points

        for (int i = 0; i < len; i++) {
            for (int j = i + 1; j < len; j++) {

                Point p1 = listResult.get(i);
                Point p2 = listResult.get(j);

                double diff = Point.getDifference(p1, p2);
                if (maxDiff < diff) {
                    maxDiffPoints[0] = p1;
                    maxDiffPoints[1] = p2;
                    maxDiff = diff;
                }

            }
        }
        // farthestPairList will contain both desired points
        List<Point> farthestPairList = new ArrayList<Point>();
        farthestPairList.add(maxDiffPoints[0]);
        farthestPairList.add(maxDiffPoints[1]);

        // delete the result folder on HDFS if it already exists.
        if (fs.exists(hdfs.mergePaths(hdfs, new Path(p.getProperty("farthest_pair_result")))))
            fs.delete(hdfs.mergePaths(hdfs, new Path(p.getProperty("farthest_pair_result"))), true);

        // save the farthestpair points to a text file
        sc.parallelize(farthestPairList).saveAsTextFile(p.getProperty("HDFS_URL") + p.getProperty("farthest_pair_result"));

        System.out.println("farthest pair points::  \n" + maxDiffPoints[0] + "\n" + maxDiffPoints[1]);
    }

    // calculate the direction in which the lines joining these points is
    // heading to.
    // 0 - collinear
    // > 0 - clockwise
    // < 0 - counter-clockwise
    public static double ccw(Point a, Point b, Point c) {
        double x1, y1, x2, y2, x3, y3;
        x1 = a.getX();
        y1 = a.getY();

        x2 = b.getX();
        y2 = b.getY();

        x3 = c.getX();
        y3 = c.getY();

        return (x2 - x1) * (y3 - y1) - (y2 - y1) * (x3 - x1);
    }
}
