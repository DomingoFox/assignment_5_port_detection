# AIS Port Detection Project

This project processes raw AIS (Automatic Identification System) vessel data to detect maritime ports using parallel computation, geospatial filtering, and DBSCAN clustering. The detected ports are visualized on an interactive map.

## Project Structure

- `task_1_filter_noise.py`: Initial big data handling in parallel, which prepares us later for port detection.
- `task_2_3_4_port_detection.py`: Clustering, port size detection and map creation using processed data from task 1.
- `files/`: Directory containing the AIS dataset (not included in repo).
- `files/cleaned/`: Directory to write cleaned files from task 1.
- `helpers/logger`: A simple logger.

## Methodology

1. **Data Ingestion**: Reads large AIS data CSV (5GB+) efficiently using PySpark.
2. **Filtering**: Removes invalid or noisy coordinates, duplicate values, non port value by navigational status.
3. **Clustering**: Applies DBSCAN to identify clusters of vessel stops, indicating ports.
4. **Visualization**: Uses Folium to plot port clusters and noise points on an interactive map.

## Key Features
- Scalable: Uses PySpark for large-scale parallel processing.
- Geospatial: DBSCAN with haversine distance and dynamic epsilon.
- Visualization: Interactive maps with cluster scaling based on unique MMSI counts.

## How to Run

1. Change the initial file path in filter_noise file at the bottom.
2. Make sure a new file was created in /files/cleaned/ directory.
3. Running the port detection inspect logs:
Logs example:
2025-06-01 10:53:34 - INFO - Number of clusters (excluding noise): 85
2025-06-01 10:53:34 - INFO - Number of noise points: 114
2025-06-01 10:53:34 - INFO - Detecting port sizes.
2025-06-01 10:53:34 - INFO - Saving and opening ports map.

4. Port map will be opened in the browser automatically.
