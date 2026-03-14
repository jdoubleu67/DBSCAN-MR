package com.dbscan;

import com.dbscan.clustering.ClusterMerger;
import com.dbscan.clustering.LocalDBSCAN;
import com.dbscan.model.Partition;
import com.dbscan.model.Point;
import com.dbscan.partition.PRBPPartitioner;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public class DBSCANMR {
    private double eps;
    private int minPts;
    private double theta;  //balance parameter
    private int beta;      //Minimum partition size
    private int numPartitions;
    private ExecutorService executorService;

    public DBSCANMR(double eps, int minPts, int numPartitions) {
        this(eps, minPts, numPartitions, 0.1, 1000);
    }

    public DBSCANMR(double eps, int minPts, int numPartitions, double theta, int beta) {
        this.eps = eps;
        this.minPts = minPts;
        this.numPartitions = numPartitions;
        this.theta = theta;
        this.beta = beta;
        this.executorService = Executors.newFixedThreadPool(
                Math.min(numPartitions, Runtime.getRuntime().availableProcessors())
        );
    }

    public DBSCANMRResult cluster(List<Point> dataset) {
        long startTime = System.currentTimeMillis();

        //PRBP
        System.out.println("Phase 1: Partitioning data with PRBP...");
        long partitionStart = System.currentTimeMillis();
        List<Partition> partitions = partitionData(dataset);
        long partitionTime = System.currentTimeMillis() - partitionStart;
        System.out.println("Partitioning completed in " + partitionTime + "ms");
        System.out.println("Created " + partitions.size() + " partitions");

        //DBSCAN-Map
        System.out.println("\nPhase 2: Local clustering on partitions...");
        long mapStart = System.currentTimeMillis();
        Map<Integer, LocalDBSCAN.ClusteringResult> localResults = performLocalClustering(partitions);
        long mapTime = System.currentTimeMillis() - mapStart;
        System.out.println("Local clustering completed in " + mapTime + "ms");

        //DBSCAN-Reduce
        System.out.println("\nPhase 3: Collecting boundary points...");
        long reduceStart = System.currentTimeMillis();
        List<ClusterMerger.BoundaryPointInfo> boundaryPoints = collectBoundaryPoints(partitions);
        long reduceTime = System.currentTimeMillis() - reduceStart;
        System.out.println("Collected " + boundaryPoints.size() + " boundary point records");

        //Merge clusters
        System.out.println("\nPhase 4: Merging clusters...");
        long mergeStart = System.currentTimeMillis();
        ClusterMerger merger = new ClusterMerger();
        ClusterMerger.MergeResult mergeResult = merger.mergeClusters(boundaryPoints);
        long mergeTime = System.currentTimeMillis() - mergeStart;
        System.out.println("Cluster merging completed in " + mergeTime + "ms");

        //Relabel
        System.out.println("\nPhase 5: Relabeling points...");
        long relabelStart = System.currentTimeMillis();
        relabelPoints(partitions, mergeResult);
        long relabelTime = System.currentTimeMillis() - relabelStart;
        System.out.println("Relabeling completed in " + relabelTime + "ms");


        Map<Integer, List<Point>> finalClusters = collectFinalClusters(dataset);

        long totalTime = System.currentTimeMillis() - startTime;

        DBSCANMRResult result = new DBSCANMRResult(
                finalClusters,
                partitions.size(),
                partitionTime,
                mapTime,
                reduceTime,
                mergeTime,
                relabelTime,
                totalTime
        );

        System.out.println("\n" + result);

        return result;
    }


    private List<Partition> partitionData(List<Point> dataset) {
        PRBPPartitioner partitioner = new PRBPPartitioner(eps, theta, beta);
        return partitioner.partition(dataset, numPartitions);
    }


    private Map<Integer, LocalDBSCAN.ClusteringResult> performLocalClustering(
            List<Partition> partitions) {

        Map<Integer, LocalDBSCAN.ClusteringResult> results = new ConcurrentHashMap<>();
        List<Future<?>> futures = new ArrayList<>();

        for (Partition partition : partitions) {
            Future<?> future = executorService.submit(() -> {
                List<Point> extendedPoints = getExtendedPartitionPoints(partition, partitions);

                LocalDBSCAN dbscan = new LocalDBSCAN(eps, minPts);
                LocalDBSCAN.ClusteringResult result = dbscan.cluster(extendedPoints);

                results.put(partition.getPartitionId(), result);

                System.out.println("Partition " + partition.getPartitionId() +
                        ": " + result.getClusterCount() + " clusters, " +
                        result.getNoiseCount() + " noise points");
            });
            futures.add(future);
        }

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        return results;
    }


    private List<Point> getExtendedPartitionPoints(Partition partition, List<Partition> allPartitions) {
        List<Point> extendedPoints = new ArrayList<>(partition.getPoints());

        for (Partition otherPartition : allPartitions) {
            if (otherPartition.getPartitionId() == partition.getPartitionId()) {
                continue;
            }

            if (partition.getBoundingBox().intersects(otherPartition.getExtendedBoundingBox())) {
                for (Point point : otherPartition.getPoints()) {
                    if (partition.isInBoundaryRegion(point, eps)) {
                        Point boundaryPoint = new Point(point);
                        boundaryPoint.setBoundary(true);
                        extendedPoints.add(boundaryPoint);
                    }
                }
            }
        }

        return extendedPoints;
    }


    private List<ClusterMerger.BoundaryPointInfo> collectBoundaryPoints(List<Partition> partitions) {
        List<ClusterMerger.BoundaryPointInfo> boundaryPoints = new ArrayList<>();

        for (Partition partition : partitions) {
            for (Point point : partition.getPoints()) {
                if (point.isBoundary() || partition.isInBoundaryRegion(point, eps)) {
                    boundaryPoints.add(new ClusterMerger.BoundaryPointInfo(
                            point.getIndex(),
                            partition.getPartitionId(),
                            point.getClusterId(),
                            point.isCore()
                    ));
                }
            }
        }

        return boundaryPoints;
    }

    private void relabelPoints(List<Partition> partitions, ClusterMerger.MergeResult mergeResult) {
        Map<ClusterMerger.ClusterId, Integer> globalClusterIds = new HashMap<>();
        int nextGlobalId = 0;

        for (Partition partition : partitions) {
            for (Point point : partition.getPoints()) {
                if (point.getClusterId() != -1) {
                    ClusterMerger.ClusterId mergedId = mergeResult.getMergedClusterId(
                            partition.getPartitionId(),
                            point.getClusterId()
                    );

                    if (!globalClusterIds.containsKey(mergedId)) {
                        globalClusterIds.put(mergedId, nextGlobalId++);
                    }

                    point.setClusterId(globalClusterIds.get(mergedId));
                }
            }
        }
    }


    private Map<Integer, List<Point>> collectFinalClusters(List<Point> dataset) {
        Map<Integer, List<Point>> clusters = new HashMap<>();

        for (Point point : dataset) {
            int clusterId = point.getClusterId();
            if (clusterId != -1) {
                clusters.computeIfAbsent(clusterId, k -> new ArrayList<>()).add(point);
            }
        }

        return clusters;
    }


    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }


    public static class DBSCANMRResult {
        private Map<Integer, List<Point>> clusters;
        private int numPartitions;
        private long partitionTime;
        private long mapTime;
        private long reduceTime;
        private long mergeTime;
        private long relabelTime;
        private long totalTime;

        public DBSCANMRResult(Map<Integer, List<Point>> clusters, int numPartitions,
                              long partitionTime, long mapTime, long reduceTime,
                              long mergeTime, long relabelTime, long totalTime) {
            this.clusters = clusters;
            this.numPartitions = numPartitions;
            this.partitionTime = partitionTime;
            this.mapTime = mapTime;
            this.reduceTime = reduceTime;
            this.mergeTime = mergeTime;
            this.relabelTime = relabelTime;
            this.totalTime = totalTime;
        }

        public Map<Integer, List<Point>> getClusters() {
            return clusters;
        }

        public int getNumClusters() {
            return clusters.size();
        }

        public int getTotalPoints() {
            return clusters.values().stream().mapToInt(List::size).sum();
        }

        @Override
        public String toString() {
            return "DBSCAN-MR Results:\n" +
                    "==================\n" +
                    "Number of clusters: " + getNumClusters() + "\n" +
                    "Total points clustered: " + getTotalPoints() + "\n" +
                    "Number of partitions: " + numPartitions + "\n" +
                    "\nTiming Breakdown:\n" +
                    "  Partition time: " + partitionTime + "ms\n" +
                    "  Map time: " + mapTime + "ms\n" +
                    "  Reduce time: " + reduceTime + "ms\n" +
                    "  Merge time: " + mergeTime + "ms\n" +
                    "  Relabel time: " + relabelTime + "ms\n" +
                    "  Total time: " + totalTime + "ms";
        }
    }
}
