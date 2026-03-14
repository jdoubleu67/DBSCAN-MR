package com.dbscan.clustering;

import com.dbscan.model.Point;
import com.dbscan.spatial.KDTree;

import java.util.*;

public class LocalDBSCAN {
    private double eps;
    private int minPts;
    private List<Point> points;
    private KDTree kdTree;
    private int nextClusterId;

    public LocalDBSCAN(double eps, int minPts) {
        this.eps = eps;
        this.minPts = minPts;
        this.nextClusterId = 0;
    }


    public ClusteringResult cluster(List<Point> partitionPoints) {
        if (partitionPoints == null || partitionPoints.isEmpty()) {
            return new ClusteringResult(new ArrayList<>(), new HashMap<>());
        }

        this.points = partitionPoints;
        this.kdTree = new KDTree(partitionPoints);
        this.nextClusterId = 0;

        for (Point point : points) {
            point.setVisited(false);
            point.setClusterId(-1);
            point.setCore(false);
        }

        for (Point point : points) {
            if (!point.isVisited()) {
                point.setVisited(true);

                List<Point> neighbors = kdTree.rangeQuery(point, eps);

                if (neighbors.size() >= minPts) {
                    point.setCore(true);
                    expandCluster(point, neighbors, nextClusterId);
                    nextClusterId++;
                }
            }
        }

        Map<Integer, List<Point>> clusters = new HashMap<>();
        List<Point> noise = new ArrayList<>();

        for (Point point : points) {
            int clusterId = point.getClusterId();
            if (clusterId == -1) {
                noise.add(point);
            } else {
                clusters.computeIfAbsent(clusterId, k -> new ArrayList<>()).add(point);
            }
        }

        return new ClusteringResult(noise, clusters);
    }


    private void expandCluster(Point corePoint, List<Point> neighbors, int clusterId) {
        corePoint.setClusterId(clusterId);

        Queue<Point> queue = new LinkedList<>(neighbors);
        Set<Point> processed = new HashSet<>();
        processed.add(corePoint);

        while (!queue.isEmpty()) {
            Point current = queue.poll();

            if (processed.contains(current)) {
                continue;
            }
            processed.add(current);

            if (!current.isVisited()) {
                current.setVisited(true);

                List<Point> currentNeighbors = kdTree.rangeQuery(current, eps);

                if (currentNeighbors.size() >= minPts) {
                    current.setCore(true);

                    for (Point neighbor : currentNeighbors) {
                        if (!processed.contains(neighbor)) {
                            queue.offer(neighbor);
                        }
                    }
                }
            }

            if (current.getClusterId() == -1) {
                current.setClusterId(clusterId);
            }
        }
    }


    public static class ClusteringResult {
        private List<Point> noise;
        private Map<Integer, List<Point>> clusters;

        public ClusteringResult(List<Point> noise, Map<Integer, List<Point>> clusters) {
            this.noise = noise;
            this.clusters = clusters;
        }

        public List<Point> getNoise() {
            return noise;
        }

        public Map<Integer, List<Point>> getClusters() {
            return clusters;
        }

        public int getClusterCount() {
            return clusters.size();
        }

        public int getNoiseCount() {
            return noise.size();
        }

        @Override
        public String toString() {
            return "ClusteringResult{" +
                    "clusterCount=" + clusters.size() +
                    ", noiseCount=" + noise.size() +
                    ", totalPoints=" + (noise.size() + clusters.values().stream()
                    .mapToInt(List::size).sum()) +
                    '}';
        }
    }
}