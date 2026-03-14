package com.dbscan.partition;

import com.dbscan.model.BoundingBox;
import com.dbscan.model.Partition;
import com.dbscan.model.Point;

import java.util.*;


public class PRBPPartitioner {
    private double eps;
    private double theta; // Load balance parameter (0 < theta < 0.5)
    private int beta;     // Minimum partition size
    private int dimension;

    public PRBPPartitioner(double eps, double theta, int beta) {
        this.eps = eps;
        this.theta = theta;
        this.beta = beta;
    }


    public List<Partition> partition(List<Point> dataset, int targetPartitions) {
        if (dataset == null || dataset.isEmpty()) {
            throw new IllegalArgumentException("Dataset cannot be null or empty");
        }

        this.dimension = dataset.get(0).getDimension();

        List<PartitionInfo> partitionInfos = new ArrayList<>();
        partitionInfos.add(new PartitionInfo(dataset, dimension));

        int partitionIdCounter = 0;


        while (partitionInfos.size() < targetPartitions) {
            boolean splitOccurred = false;

            for (int i = 0; i < partitionInfos.size(); i++) {
                PartitionInfo partInfo = partitionInfos.get(i);

                if (!partInfo.isProcessed() && partInfo.getPoints().size() >= beta) {
                    SplitResult splitResult = findBestSplit(partInfo);

                    if (splitResult != null) {
                        partitionInfos.remove(i);
                        partitionInfos.addAll(splitResult.partitions);
                        splitOccurred = true;
                        break;
                    } else {
                        partInfo.setProcessed(true);
                    }
                }
            }

            if (!splitOccurred) {
                break;
            }
        }


        List<Partition> finalPartitions = new ArrayList<>();
        for (int i = 0; i < partitionInfos.size(); i++) {
            Partition partition = new Partition(i, dimension);
            partition.addAllPoints(partitionInfos.get(i).getPoints());
            partition.extendBoundary(eps);
            finalPartitions.add(partition);
        }

        return finalPartitions;
    }


    private SplitResult findBestSplit(PartitionInfo partInfo) {
        List<Point> points = partInfo.getPoints();

        if (points.size() < beta) {
            return null;
        }


        Map<Integer, List<Slice>> dimensionSlices = buildSlices(points);


        Slice bestSlice = null;
        int bestDimension = -1;
        int minBoundaryPoints = Integer.MAX_VALUE;

        for (int dim = 0; dim < dimension; dim++) {
            List<Slice> slices = dimensionSlices.get(dim);

            if (slices.size() < 3) {
                continue;
            }


            for (int i = 1; i < slices.size() - 1; i++) {
                Slice slice = slices.get(i);


                int pointsLeft = slice.cumulativeCount;
                int pointsRight = points.size() - pointsLeft;

                if (pointsLeft >= points.size() * theta &&
                        pointsRight >= points.size() * theta) {

                    if (slice.count < minBoundaryPoints) {
                        minBoundaryPoints = slice.count;
                        bestSlice = slice;
                        bestDimension = dim;
                    }
                }
            }
        }

        if (bestSlice == null) {
            return null;
        }


        return splitPartition(points, bestDimension, bestSlice);
    }


    private Map<Integer, List<Slice>> buildSlices(List<Point> points) {
        Map<Integer, List<Slice>> dimensionSlices = new HashMap<>();

        for (int dim = 0; dim < dimension; dim++) {
            List<Slice> slices = buildSlicesForDimension(points, dim);
            dimensionSlices.put(dim, slices);
        }

        return dimensionSlices;
    }


    private List<Slice> buildSlicesForDimension(List<Point> points, int dim) {

        List<Point> sortedPoints = new ArrayList<>(points);
        sortedPoints.sort(Comparator.comparingDouble(p -> p.getCoordinate(dim)));

        if (sortedPoints.isEmpty()) {
            return new ArrayList<>();
        }

        double minValue = sortedPoints.get(0).getCoordinate(dim);
        double maxValue = sortedPoints.get(sortedPoints.size() - 1).getCoordinate(dim);

        double sliceWidth = 2 * eps;
        int numSlices = (int) Math.ceil((maxValue - minValue) / sliceWidth) + 1;

        List<Slice> slices = new ArrayList<>();
        int cumulativeCount = 0;
        int currentSliceIdx = 0;
        int currentSliceCount = 0;
        double currentSliceStart = minValue;

        for (Point point : sortedPoints) {
            double value = point.getCoordinate(dim);
            int sliceIdx = (int) ((value - minValue) / sliceWidth);


            while (sliceIdx > currentSliceIdx) {
                Slice slice = new Slice(currentSliceIdx, currentSliceStart,
                        currentSliceStart + sliceWidth,
                        currentSliceCount, cumulativeCount);
                slices.add(slice);

                currentSliceIdx++;
                currentSliceStart += sliceWidth;
                currentSliceCount = 0;
            }

            currentSliceCount++;
            cumulativeCount++;
        }


        Slice lastSlice = new Slice(currentSliceIdx, currentSliceStart,
                currentSliceStart + sliceWidth,
                currentSliceCount, cumulativeCount);
        slices.add(lastSlice);

        return slices;
    }


    private SplitResult splitPartition(List<Point> points, int splitDim, Slice splitSlice) {
        List<Point> leftPoints = new ArrayList<>();
        List<Point> rightPoints = new ArrayList<>();

        for (Point point : points) {
            double value = point.getCoordinate(splitDim);

            if (value <= splitSlice.end) {
                leftPoints.add(point);
            } else {
                rightPoints.add(point);
            }
        }

        List<PartitionInfo> resultPartitions = new ArrayList<>();
        resultPartitions.add(new PartitionInfo(leftPoints, dimension));
        resultPartitions.add(new PartitionInfo(rightPoints, dimension));

        return new SplitResult(resultPartitions);
    }


    private static class Slice {
        int index;
        double start;
        double end;
        int count;              // Number of points in this slice
        int cumulativeCount;

        Slice(int index, double start, double end, int count, int cumulativeCount) {
            this.index = index;
            this.start = start;
            this.end = end;
            this.count = count;
            this.cumulativeCount = cumulativeCount;
        }
    }


    private static class PartitionInfo {
        private List<Point> points;
        private boolean processed;
        private int dimension;

        PartitionInfo(List<Point> points, int dimension) {
            this.points = points;
            this.processed = false;
            this.dimension = dimension;
        }

        List<Point> getPoints() {
            return points;
        }

        boolean isProcessed() {
            return processed;
        }

        void setProcessed(boolean processed) {
            this.processed = processed;
        }
    }


    private static class SplitResult {
        List<PartitionInfo> partitions;

        SplitResult(List<PartitionInfo> partitions) {
            this.partitions = partitions;
        }
    }
}