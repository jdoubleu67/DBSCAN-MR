package com.dbscan.spatial;

import com.dbscan.model.Point;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class KDTree {
    private KDNode root;
    private int dimension;

    private static class KDNode {
        Point point;
        KDNode left;
        KDNode right;
        int splitDimension;

        KDNode(Point point, int splitDimension) {
            this.point = point;
            this.splitDimension = splitDimension;
        }
    }

    public KDTree(List<Point> points) {
        if (points == null || points.isEmpty()) {
            throw new IllegalArgumentException("Points list cannot be null or empty");
        }
        this.dimension = points.get(0).getDimension();
        this.root = buildTree(new ArrayList<>(points), 0);
    }

    private KDNode buildTree(List<Point> points, int depth) {
        if (points.isEmpty()) {
            return null;
        }

        int splitDim = depth % dimension;


        Collections.sort(points, Comparator.comparingDouble(p -> p.getCoordinate(splitDim)));

        int medianIndex = points.size() / 2;
        Point medianPoint = points.get(medianIndex);

        KDNode node = new KDNode(medianPoint, splitDim);


        node.left = buildTree(points.subList(0, medianIndex), depth + 1);
        node.right = buildTree(points.subList(medianIndex + 1, points.size()), depth + 1);

        return node;
    }


    public List<Point> rangeQuery(Point queryPoint, double eps) {
        List<Point> result = new ArrayList<>();
        rangeQueryRecursive(root, queryPoint, eps, result);
        return result;
    }

    private void rangeQueryRecursive(KDNode node, Point queryPoint, double eps, List<Point> result) {
        if (node == null) {
            return;
        }


        if (node.point.distance(queryPoint) <= eps) {
            result.add(node.point);
        }

        int splitDim = node.splitDimension;
        double diff = queryPoint.getCoordinate(splitDim) - node.point.getCoordinate(splitDim);


        KDNode nearNode = (diff < 0) ? node.left : node.right;
        KDNode farNode = (diff < 0) ? node.right : node.left;


        rangeQueryRecursive(nearNode, queryPoint, eps, result);


        if (Math.abs(diff) <= eps) {
            rangeQueryRecursive(farNode, queryPoint, eps, result);
        }
    }


    public List<Point> kNearestNeighbors(Point queryPoint, int k) {
        List<PointDistance> neighbors = new ArrayList<>();
        knnRecursive(root, queryPoint, k, neighbors);

        List<Point> result = new ArrayList<>();
        for (PointDistance pd : neighbors) {
            result.add(pd.point);
        }
        return result;
    }

    private void knnRecursive(KDNode node, Point queryPoint, int k, List<PointDistance> neighbors) {
        if (node == null) {
            return;
        }

        double distance = node.point.distance(queryPoint);


        if (neighbors.size() < k) {
            neighbors.add(new PointDistance(node.point, distance));
            Collections.sort(neighbors);
        } else if (distance < neighbors.get(neighbors.size() - 1).distance) {
            neighbors.set(neighbors.size() - 1, new PointDistance(node.point, distance));
            Collections.sort(neighbors);
        }

        int splitDim = node.splitDimension;
        double diff = queryPoint.getCoordinate(splitDim) - node.point.getCoordinate(splitDim);

        KDNode nearNode = (diff < 0) ? node.left : node.right;
        KDNode farNode = (diff < 0) ? node.right : node.left;

        knnRecursive(nearNode, queryPoint, k, neighbors);


        if (neighbors.size() < k || Math.abs(diff) < neighbors.get(neighbors.size() - 1).distance) {
            knnRecursive(farNode, queryPoint, k, neighbors);
        }
    }

    private static class PointDistance implements Comparable<PointDistance> {
        Point point;
        double distance;

        PointDistance(Point point, double distance) {
            this.point = point;
            this.distance = distance;
        }

        @Override
        public int compareTo(PointDistance other) {
            return Double.compare(this.distance, other.distance);
        }
    }
}