package com.dbscan.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Partition implements Serializable {
    private static final long serialVersionUID = 1L;

    private int partitionId;
    private List<Point> points;
    private BoundingBox boundingBox;
    private BoundingBox extendedBoundingBox;
    private int dimension;

    public Partition(int partitionId, int dimension) {
        this.partitionId = partitionId;
        this.dimension = dimension;
        this.points = new ArrayList<>();
        this.boundingBox = new BoundingBox(dimension);
    }

    public void addPoint(Point point) {
        points.add(point);
        point.setPartitionId(partitionId);
        boundingBox.extend(point);
    }

    public void addAllPoints(List<Point> pointList) {
        for (Point p : pointList) {
            addPoint(p);
        }
    }

    public void extendBoundary(double eps) {
        extendedBoundingBox = new BoundingBox(boundingBox);
        extendedBoundingBox.expand(eps);
    }

    public boolean isInBoundaryRegion(Point point, double eps) {
        if (extendedBoundingBox == null) {
            extendBoundary(eps);
        }


        for (int dim = 0; dim < dimension; dim++) {
            double coord = point.getCoordinate(dim);
            double min = boundingBox.getMin(dim);
            double max = boundingBox.getMax(dim);

            if (Math.abs(coord - min) <= eps || Math.abs(coord - max) <= eps) {
                return true;
            }
        }
        return false;
    }

    public boolean contains(Point point) {
        return boundingBox.contains(point);
    }

    public boolean intersects(BoundingBox other) {
        return boundingBox.intersects(other);
    }

    public int size() {
        return points.size();
    }


    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public List<Point> getPoints() {
        return points;
    }

    public void setPoints(List<Point> points) {
        this.points = points;
    }

    public BoundingBox getBoundingBox() {
        return boundingBox;
    }

    public BoundingBox getExtendedBoundingBox() {
        return extendedBoundingBox;
    }

    public int getDimension() {
        return dimension;
    }

    @Override
    public String toString() {
        return "Partition{" +
                "partitionId=" + partitionId +
                ", pointCount=" + points.size() +
                ", boundingBox=" + boundingBox +
                '}';
    }
}
