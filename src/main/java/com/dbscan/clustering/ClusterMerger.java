package com.dbscan.clustering;

import com.dbscan.model.Point;

import java.util.*;


public class ClusterMerger {


    public MergeResult mergeClusters(List<BoundaryPointInfo> boundaryPoints) {

        Map<ClusterId, Set<ClusterId>> mergeGraph = buildMergeGraph(boundaryPoints);

        List<Set<ClusterId>> connectedComponents = findConnectedComponents(mergeGraph);

        Map<ClusterId, ClusterId> mergeMapping = createMergeMapping(connectedComponents);

        return new MergeResult(mergeMapping);
    }


    private Map<ClusterId, Set<ClusterId>> buildMergeGraph(List<BoundaryPointInfo> boundaryPoints) {
        Map<ClusterId, Set<ClusterId>> graph = new HashMap<>();

        Map<Long, List<BoundaryPointInfo>> pointGroups = new HashMap<>();
        for (BoundaryPointInfo info : boundaryPoints) {
            pointGroups.computeIfAbsent(info.pointIndex, k -> new ArrayList<>()).add(info);
        }

        for (List<BoundaryPointInfo> group : pointGroups.values()) {
            boolean hasCore = group.stream().anyMatch(BoundaryPointInfo::isCore);

            if (hasCore && group.size() > 1) {
                List<ClusterId> clusterIds = new ArrayList<>();
                for (BoundaryPointInfo info : group) {
                    if (info.clusterId != -1) {
                        clusterIds.add(new ClusterId(info.partitionId, info.clusterId));
                    }
                }

                if (clusterIds.size() > 1) {
                    for (int i = 0; i < clusterIds.size(); i++) {
                        ClusterId id1 = clusterIds.get(i);
                        graph.computeIfAbsent(id1, k -> new HashSet<>());

                        for (int j = i + 1; j < clusterIds.size(); j++) {
                            ClusterId id2 = clusterIds.get(j);
                            graph.get(id1).add(id2);
                            graph.computeIfAbsent(id2, k -> new HashSet<>()).add(id1);
                        }
                    }
                }
            }
        }

        return graph;
    }


    private List<Set<ClusterId>> findConnectedComponents(Map<ClusterId, Set<ClusterId>> graph) {
        List<Set<ClusterId>> components = new ArrayList<>();
        Set<ClusterId> visited = new HashSet<>();

        for (ClusterId node : graph.keySet()) {
            if (!visited.contains(node)) {
                Set<ClusterId> component = new HashSet<>();
                dfs(node, graph, visited, component);
                components.add(component);
            }
        }

        return components;
    }


    private void dfs(ClusterId node, Map<ClusterId, Set<ClusterId>> graph,
                     Set<ClusterId> visited, Set<ClusterId> component) {
        visited.add(node);
        component.add(node);

        Set<ClusterId> neighbors = graph.get(node);
        if (neighbors != null) {
            for (ClusterId neighbor : neighbors) {
                if (!visited.contains(neighbor)) {
                    dfs(neighbor, graph, visited, component);
                }
            }
        }
    }


    private Map<ClusterId, ClusterId> createMergeMapping(List<Set<ClusterId>> components) {
        Map<ClusterId, ClusterId> mapping = new HashMap<>();

        for (Set<ClusterId> component : components) {
            if (component.isEmpty()) {
                continue;
            }

            ClusterId representative = component.stream()
                    .min(Comparator.comparingInt(ClusterId::getPartitionId)
                            .thenComparingInt(ClusterId::getLocalClusterId))
                    .orElseThrow();

            for (ClusterId clusterId : component) {
                mapping.put(clusterId, representative);
            }
        }

        return mapping;
    }


    public static class BoundaryPointInfo {
        private long pointIndex;
        private int partitionId;
        private int clusterId;
        private boolean isCore;

        public BoundaryPointInfo(long pointIndex, int partitionId, int clusterId, boolean isCore) {
            this.pointIndex = pointIndex;
            this.partitionId = partitionId;
            this.clusterId = clusterId;
            this.isCore = isCore;
        }

        public long getPointIndex() {
            return pointIndex;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public int getClusterId() {
            return clusterId;
        }

        public boolean isCore() {
            return isCore;
        }
    }


    public static class ClusterId {
        private int partitionId;
        private int localClusterId;

        public ClusterId(int partitionId, int localClusterId) {
            this.partitionId = partitionId;
            this.localClusterId = localClusterId;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public int getLocalClusterId() {
            return localClusterId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClusterId clusterId = (ClusterId) o;
            return partitionId == clusterId.partitionId &&
                    localClusterId == clusterId.localClusterId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionId, localClusterId);
        }

        @Override
        public String toString() {
            return "P" + partitionId + "C" + localClusterId;
        }
    }


    public static class MergeResult {
        private Map<ClusterId, ClusterId> mergeMapping;

        public MergeResult(Map<ClusterId, ClusterId> mergeMapping) {
            this.mergeMapping = mergeMapping;
        }

        public Map<ClusterId, ClusterId> getMergeMapping() {
            return mergeMapping;
        }

        public ClusterId getMergedClusterId(int partitionId, int localClusterId) {
            ClusterId key = new ClusterId(partitionId, localClusterId);
            return mergeMapping.getOrDefault(key, key);
        }
    }
}
