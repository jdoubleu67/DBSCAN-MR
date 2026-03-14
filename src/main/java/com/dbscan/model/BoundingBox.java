package com.dbscan.model;

import java.io.Serializable;
import java.util.Arrays;


public class BoundingBox implements Serializable {
    private static final long serialVersionUID = 1L;

    private double[] min;
    private double[] max;
    private int dimension;

    public BoundingBox(int dimension) {
        this.dimension = dimension;
        this.min = new double[dimension];
        this.max = new double[dimension];

        Arrays.fill(min, Double.POSITIVE_INFINITY);
        Arrays.fill(max, Double.NEGATIVE_INFINITY);
    }

    public BoundingBox(BoundingBox other) {
        this.dimension = other.dimension;
        this.min = Arrays.copyOf(other.min, other.dimension);
        this.max = Arrays.copyOf(other.max, other.dimension);
    }

    public BoundingBox(double[] min, double[] max) {
        if (min.length != max.length) {
            throw new IllegalArgumentException("Min and max must have same dimensions");
        }
        this.dimension = min.length;
        this.min = Arrays.copyOf(min, dimension);
        this.max = Arrays.copyOf(max, dimension);
    }

    public void extend(Point point) {
        for (int i = 0; i < dimension; i++) {
            double coord = point.getCoordinate(i);
            if (coord < min[i]) {
                min[i] = coord;
            }
            if (coord > max[i]) {
                max[i] = coord;
            }
        }
    }

    public void expand(double margin) {
        for (int i = 0; i < dimension; i++) {
            min[i] -= margin;
            max[i] += margin;
        }
    }

    public boolean contains(Point point) {
        for (int i = 0; i < dimension; i++) {
            double coord = point.getCoordinate(i);
            if (coord < min[i] || coord > max[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean intersects(BoundingBox other) {
        for (int i = 0; i < dimension; i++) {
            if (max[i] < other.min[i] || min[i] > other.max[i]) {
                return false;
            }
        }
        return true;
    }

    public double getWidth(int dimension) {
        return max[dimension] - min[dimension];
    }

    public double getCenter(int dimension) {
        return (min[dimension] + max[dimension]) / 2.0;
    }

    public double getVolume() {
        double volume = 1.0;
        for (int i = 0; i < dimension; i++) {
            volume *= (max[i] - min[i]);
        }
        return volume;
    }


    public double[] getMin() {
        return min;
    }

    public double getMin(int dimension) {
        return min[dimension];
    }

    public double[] getMax() {
        return max;
    }

    public double getMax(int dimension) {
        return max[dimension];
    }

    public int getDimension() {
        return dimension;
    }

    @Override
    public String toString() {
        return "BoundingBox{" +
                "min=" + Arrays.toString(min) +
                ", max=" + Arrays.toString(max) +
                '}';
    }
}