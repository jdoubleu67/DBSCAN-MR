package com.dbscan.model;

import java.io.Serializable;
import java.util.Arrays;


public class Point implements Serializable {
    private static final long serialVersionUID = 1L;

    private long index;
    private double[] coordinates;
    private int clusterId;
    private boolean isCore;
    private boolean isBoundary;
    private int partitionId;
    private boolean visited;

    public Point(long index, double[] coordinates) {
        this.index = index;
        this.coordinates = coordinates;
        this.clusterId = -1; // -1 = noise
        this.isCore = false;
        this.isBoundary = false;
        this.partitionId = -1;
        this.visited = false;
    }

    public Point(Point other) {
        this.index = other.index;
        this.coordinates = Arrays.copyOf(other.coordinates, other.coordinates.length);
        this.clusterId = other.clusterId;
        this.isCore = other.isCore;
        this.isBoundary = other.isBoundary;
        this.partitionId = other.partitionId;
        this.visited = other.visited;
    }

    public double distance(Point other) {
        if (this.coordinates.length != other.coordinates.length) {
            throw new IllegalArgumentException("Points must have same dimensions");
        }

        double sum = 0.0;
        for (int i = 0; i < coordinates.length; i++) {
            double diff = coordinates[i] - other.coordinates[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public int getDimension() {
        return coordinates.length;
    }

    public double getCoordinate(int dimension) {
        return coordinates[dimension];
    }

    public void setCoordinate(int dimension, double value) {
        coordinates[dimension] = value;
    }


    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public double[] getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(double[] coordinates) {
        this.coordinates = coordinates;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public boolean isCore() {
        return isCore;
    }

    public void setCore(boolean core) {
        isCore = core;
    }

    public boolean isBoundary() {
        return isBoundary;
    }

    public void setBoundary(boolean boundary) {
        isBoundary = boundary;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public boolean isVisited() {
        return visited;
    }

    public void setVisited(boolean visited) {
        this.visited = visited;
    }

    @Override
    public String toString() {
        return "Point{" +
                "index=" + index +
                ", coordinates=" + Arrays.toString(coordinates) +
                ", clusterId=" + clusterId +
                ", isCore=" + isCore +
                ", partitionId=" + partitionId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return index == point.index;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(index);
    }
}
