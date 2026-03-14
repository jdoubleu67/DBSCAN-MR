package com.dbscan;

import com.dbscan.model.Point;
import com.dbscan.util.DataLoader;

import java.io.IOException;
import java.util.List;


public class Main {

    public static void main(String[] args) {
        try {
            String filename =
                    "data\\densired_2_shrink.csv";

            //load data
            System.out.println("Loading data from: " + filename);
            List<Point> dataset = DataLoader.loadFromCSV(filename, false);
            System.out.println("Loaded " + dataset.size() + " points");

            //set parameters
            double eps = 0.03;
            int minPts = 5;
            int numPartitions = 10;

            System.out.println("configured parameters:");
            System.out.println("  eps: " + eps);
            System.out.println("  minPts: " + minPts);
            System.out.println("  partitions: " + numPartitions);
            System.out.println();


            DBSCANMR dbscanMR = new DBSCANMR(eps, minPts, numPartitions);
            DBSCANMR.DBSCANMRResult result = dbscanMR.cluster(dataset);

            String outputFile = "result.csv";
            String summaryFile = "summary.txt";


            DataLoader.saveResults(outputFile, dataset);
            DataLoader.saveSummary(summaryFile, result);

            System.out.println("\nResults saved to:");
            System.out.println("  - " + outputFile);
            System.out.println("  - " + summaryFile);

            dbscanMR.shutdown();

            System.out.println("Clustering finished.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }





}
