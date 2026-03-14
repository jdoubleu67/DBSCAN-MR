# DBSCAN-MR

Implementation of the **DBSCAN-MR** algorithm based on:
> Dai, B.-R., & Lin, I.-C. (2012). *Efficient Map/Reduce-based DBSCAN Algorithm with Optimized Data Partition*.

---

## Requirements

- Java 11+

---

### Installation
1. ```bash
   git clone https://github.com/jdoubleu67/DBSCAN-MR.git
2. Go into IntelliJ or any IDE of your choice and open up the Project Folder


---

## Usage

1. Change the path for "filename" in Main.Java to the dataset you want to execute the algorithm on
2. Change the path for "outputFile" and summaryFile to the location you want the clustered data and the summary to be saved
3. Change the parameters "eps", "minPts" and "numPartitions" to whatever value fits your individual dataset best (Try experimenting a bit to find the best results)
4. Run the main method in the Main class

---

## Data Format

.csv file

one point per line, no header


## Parameters

| Parameter | Description | 
|-----------|-------------|
| `eps` (ε) | Neighborhood radius |
| `minPts` | Min. points for a core point |
| `numPartitions` | Number of partitions | 
| `theta` (θ) | Load balance factor| 
| `beta` (β) | Min. partition size | 

Parameters can be changed in `Main.java`:

---

## Output

| File | Content |
|------|---------|
| `results.csv` | `index, cluster_id, is_core, coordinates` per point |
| `summary.txt` | cluster count, cluster sizes |
