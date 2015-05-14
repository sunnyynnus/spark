package com.geospatial.operation2;

import java.util.Comparator;

interface PointComparator extends Comparator<Point>, java.io.Serializable {
    // empty interface which is implemented by Point class which is serializable
    // and overrides CompareTo method.
}

@SuppressWarnings({ "serial" })
public class Point implements java.io.Serializable, PointComparator, Comparable<Point> {

    public double       x;
    public static Point min = new Point();

    // static boolean forSort;

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    double y;

    // this sort is used to sort according to polar angle
    public int compareTo(Point p2) {
        // TODO Auto-generated method stub

        double a1 = Math.atan2(this.getY() - min.getY(), this.getX() - min.getX());
        double a2 = Math.atan2(p2.getY() - min.getY(), p2.getX() - min.getX());

        if (a1 < a2)
            return -1;
        else if (a1 > a2)
            return 1;
        else {
            return 0;
        }
    }

    public int compare(Point p1, Point p2) {

        if (p1.getY() > p2.getY())
            return 1;
        else if (p1.getY() < p2.getY())
            return -1;
        else {
            if (p1.getX() < p2.getX())
                return -1;
            else if (p1.getX() < p2.getX())
                return 1;
            else
                return 0;
        }

    }

    public String toString() {
        return this.getX() + "," + this.getY();
    }

    /**
     * returns euclidean distance between input points p1 and p2
     * 
     * @param p1
     * @param p2
     * @return
     */
    public static double getDifference(Point p1, Point p2) {

        return Math.sqrt(Math.pow((p1.x - p2.x), 2) + Math.pow((p1.y - p2.y), 2));

    }
}