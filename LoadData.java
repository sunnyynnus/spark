package com.geospatial.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LoadData {
    public static void main(String[] args) {

        URI uri = URI.create("hdfs://master:54310/");

        // PolygonUnionTestData.csv
        Path pathhadoop = new Path(uri);

        Path pathUnion = new Path("dataset/PolygonUnionTestData.csv");
        Path pathConvexHull = new Path("dataset/ConvexHullTestData.csv");
        Path pathPairPoints = new Path("dataset/FarthestPairandClosestPairTestData.csv");
        Path pathRangeQuery = new Path("dataset/RangeQueryTestData.csv");
        Path pathQueryWindows = new Path("dataset/QueryWindows.csv");
        Path pathQueryRectangle = new Path("dataset/QueryRectangle.csv");
        Path pathSpatialJoin = new Path("dataset/JoinQueryTestData.csv");

        Configuration confhadoop = new Configuration();
        Path dataset = Path.mergePaths(pathhadoop, new Path("dataset"));

        try {

            FileSystem filehadoop = FileSystem.get(uri, confhadoop);
            if (filehadoop.exists(dataset)) {
                filehadoop.delete(dataset, true);
            }
            filehadoop.mkdirs(dataset);
            filehadoop.copyFromLocalFile(pathUnion, dataset);
            System.out.println("Loaded Union dataset");
            filehadoop.copyFromLocalFile(pathConvexHull, dataset);
            System.out.println("Loaded Convex Hull dataset");
            filehadoop.copyFromLocalFile(pathPairPoints, dataset);
            System.out.println("Loaded Pair Points dataset");
            filehadoop.copyFromLocalFile(pathQueryRectangle, dataset);
            System.out.println("Loaded Query Rectangle Query dataset");
            filehadoop.copyFromLocalFile(pathRangeQuery, dataset);
            System.out.println("Loaded Range Query dataset");
            filehadoop.copyFromLocalFile(pathQueryWindows, dataset);
            System.out.println("Loaded Query Windows dataset");
            filehadoop.copyFromLocalFile(pathSpatialJoin, dataset);
            System.out.println("Loaded Join Query dataset");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
