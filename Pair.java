package com.geospatial.operation4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Reference :- http://rosettacode.org/wiki/Closest-pair_problem static
 * class contains a pair of points along with euclidean distance between
 * them helper class providing divide and conquer method to find two closest
 * points
 * 
 * @author team15
 *
 */
public class Pair {

	public Point point1 = null;
	public Point point2 = null;
	public double distance = 0.0;

	public Pair() {
	}

	public Pair(Point point1, Point point2) {

		this.point1 = point1;
		this.point2 = point2;
		calcDistance();
	}

	public void calcDistance() {

		this.distance = distance(point1, point2);
	}

	public static double distance(Point p1, Point p2) {

		double xdist = p2.x - p1.x;
		double ydist = p2.y - p1.y;
		if (p1.x == p2.x && p1.y == p2.y) {
			return Integer.MAX_VALUE;
		}
		return Math.hypot(xdist, ydist);
	}

	/**
	 * recursive call divide and conquer after sorting points by X and Y
	 * these method will be called for each partition
	 * 
	 * @param points
	 * @return pair of points having min. distance in that partition
	 */
	public static Pair divideAndConquer(List<? extends Point> points) {

		List<Point> pointsSortedByX = new ArrayList<Point>(points);
		sortByX(pointsSortedByX);
		List<Point> pointsSortedByY = new ArrayList<Point>(points);
		sortByY(pointsSortedByY);
		return divideAndConquer(pointsSortedByX, pointsSortedByY);
	}

	/**
	 * sort points by y-axis coordinates
	 * 
	 * @param points
	 */
	public static void sortByY(List<? extends Point> points) {

		Collections.sort(points, new Comparator<Point>() {
			public int compare(Point point1, Point point2) {

				if (point1.y < point2.y)
					return -1;
				if (point1.y > point2.y)
					return 1;
				return 0;
			}
		});
	}

	/**
	 * sort points by x-axis coordinates
	 * 
	 * @param points
	 */
	public static void sortByX(List<? extends Point> points) {

		Collections.sort(points, new Comparator<Point>() {
			public int compare(Point point1, Point point2) {

				if (point1.x < point2.x)
					return -1;
				if (point1.x > point2.x)
					return 1;
				return 0;
			}
		});
	}

	/**
	 * 
	 * @param pointsSortedByX
	 * @param pointsSortedByY
	 * @return
	 */
	private static Pair divideAndConquer(
			List<? extends Point> pointsSortedByX,
			List<? extends Point> pointsSortedByY) {

		int numPoints = pointsSortedByX.size();
		if (numPoints <= 3)
			return bruteForce(pointsSortedByX);

		int dividingIndex = numPoints >>> 1;
		List<? extends Point> leftOfCenter = pointsSortedByX.subList(0,
				dividingIndex);
		List<? extends Point> rightOfCenter = pointsSortedByX.subList(
				dividingIndex, numPoints);

		List<Point> tempList = new ArrayList<Point>(leftOfCenter);
		sortByY(tempList);
		Pair closestPair = divideAndConquer(leftOfCenter, tempList);

		tempList.clear();
		tempList.addAll(rightOfCenter);
		sortByY(tempList);
		Pair closestPairRight = divideAndConquer(rightOfCenter, tempList);

		if (closestPairRight.distance < closestPair.distance) {
			closestPair = closestPairRight;
		}

		tempList.clear();
		double shortestDistance = closestPair.distance;
		double centerX = rightOfCenter.get(0).x;
		for (Point point : pointsSortedByY)
			if (Math.abs(centerX - point.x) < shortestDistance) {
				tempList.add(point);
			}
		for (int i = 0; i < tempList.size() - 1; i++) {

			Point point1 = tempList.get(i);
			for (int j = i + 1; j < tempList.size(); j++) {

				Point point2 = tempList.get(j);
				if ((point2.y - point1.y) >= shortestDistance)
					break;
				double distance = distance(point1, point2);
				if (distance < closestPair.distance) {

					closestPair.update(point1, point2, distance);
					shortestDistance = distance;
				}
			}
		}
		return closestPair;
	}

	public void update(Point point1, Point point2, double distance) {

		this.point1 = point1;
		this.point2 = point2;
		this.distance = distance;
	}

	/**
	 * 
	 * @param points
	 * @return pair of points having min. distance in that partition using
	 *         brute force approach for <=3 points
	 */
	public static Pair bruteForce(List<? extends Point> points) {

		int numPoints = points.size();
		if (numPoints < 2)
			return null;
		Pair pair = new Pair(points.get(0), points.get(1));
		if (numPoints > 2) {

			for (int i = 0; i < numPoints - 1; i++) {

				Point point1 = points.get(i);
				for (int j = i + 1; j < numPoints; j++) {

					Point point2 = points.get(j);
					double distance = distance(point1, point2);
					if (distance < pair.distance)
						pair.update(point1, point2, distance);
				}
			}
		}
		return pair;
	}
}