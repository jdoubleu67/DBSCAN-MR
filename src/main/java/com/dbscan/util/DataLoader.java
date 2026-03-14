package com.dbscan.util;

import com.dbscan.DBSCANMR;
import com.dbscan.model.Point;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class DataLoader {


    public static List<Point> loadFromCSV(String filename, boolean hasIndex) throws IOException {
        List<Point> points = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            long index = 0;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                String[] parts = line.split(",");

                double[] coordinates;
                long pointIndex;

                if (hasIndex) {
                    pointIndex = Long.parseLong(parts[0].trim());
                    coordinates = new double[parts.length - 1];
                    for (int i = 1; i < parts.length; i++) {
                        coordinates[i - 1] = Double.parseDouble(parts[i].trim());
                    }
                } else {
                    pointIndex = index++;
                    coordinates = new double[parts.length];
                    for (int i = 0; i < parts.length; i++) {
                        coordinates[i] = Double.parseDouble(parts[i].trim());
                    }
                }

                points.add(new Point(pointIndex, coordinates));
            }
        }

        return points;
    }


    public static List<Point> loadFromSpaceSeparated(String filename) throws IOException {
        List<Point> points = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            long index = 0;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                String[] parts = line.split("\\s+");
                double[] coordinates = new double[parts.length];

                for (int i = 0; i < parts.length; i++) {
                    coordinates[i] = Double.parseDouble(parts[i].trim());
                }

                points.add(new Point(index++, coordinates));
            }
        }

        return points;
    }




    public static void saveResults(String filename, List<Point> points) throws IOException {
        try (PrintWriter pw = new PrintWriter(new FileWriter(filename))) {
            pw.println("index,cluster_id,is_core,coordinates");

            for (Point point : points) {
                pw.print(point.getIndex());
                pw.print(",");
                pw.print(point.getClusterId());
                pw.print(",");
                pw.print(point.isCore());
                pw.print(",\"");

                double[] coords = point.getCoordinates();
                for (int i = 0; i < coords.length; i++) {
                    if (i > 0) pw.print(";");
                    pw.print(coords[i]);
                }
                pw.println("\"");
            }
        }
    }


    public static void saveSummary(String filename, DBSCANMR.DBSCANMRResult result)
            throws IOException {
        try (PrintWriter pw = new PrintWriter(new FileWriter(filename))) {
            pw.println("DBSCAN-MR Clustering Summary");
            pw.println("============================");
            pw.println();
            pw.println("Number of clusters: " + result.getNumClusters());
            pw.println("Total points clustered: " + result.getTotalPoints());
            pw.println();

            pw.println("Cluster Sizes:");
            pw.println("--------------");
            for (int clusterId : result.getClusters().keySet()) {
                int size = result.getClusters().get(clusterId).size();
                pw.println("Cluster " + clusterId + ": " + size + " points");
            }
        }
    }
}