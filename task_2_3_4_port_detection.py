
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from helpers.logger import log
import folium
import webbrowser

def read_data(filepath):
    df = pd.read_csv(filepath)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True)
    return df

def cluster_vessels(df):
    coords = df[['Latitude', 'Longitude']].to_numpy()
    coords_rad = np.radians(coords)

    # DBSCAN is used for clustering.
    # Epsilon selected to give a 3km error for clusters.
    db = DBSCAN(eps=3/6371, min_samples=2, algorithm='ball_tree', metric='haversine')
    labels = db.fit_predict(coords_rad)
    df['port_cluster'] = labels
    return df

def get_port_clusters(df):
    cluster_stats = df.groupby('port_cluster')['MMSI'].nunique().reset_index()
    cluster_stats.columns = ['port_cluster', 'unique_mmsi']

    # Keeping clusters with multiple unique vessels
    valid_clusters = cluster_stats[cluster_stats['unique_mmsi'] >= 3]['port_cluster']
    df = df[df['port_cluster'].isin(valid_clusters) | (df['port_cluster'] == -1)]

    number_of_clusters = len(set(df['port_cluster'])) - 1 # Just subtracting noise cluster.
    noise_points = sum(df['port_cluster'] == -1)
    log.info(f"Number of clusters (excluding noise): {number_of_clusters}")
    log.info(f"Number of noise points: {noise_points}")
    return df

def calculate_port_sizes(df):
    # Computing number of unique vessels per port cluster (excluding noise)
    log.info('Detecting port sizes.')
    port_sizes = (
        df[df['port_cluster'] != -1]
        .groupby('port_cluster')['MMSI']
        .nunique()
        .reset_index(name='vessel_count')
    )

    # Getting cluster centers for visualization
    cluster_centers = (
        df[df['port_cluster'] != -1]
        .groupby('port_cluster')[['Latitude', 'Longitude']]
        .mean()
        .reset_index()
    )

    # Merging sizes with center points
    port_data = pd.merge(cluster_centers, port_sizes, on='port_cluster')
    return port_data

def visualize_ports(port_data, df):
        
    # Creating map
    center_lat = port_data['Latitude'].mean()
    center_lon = port_data['Longitude'].mean()
    map = folium.Map(location=[center_lat, center_lon], zoom_start=6)

    # Adding each port as a circle marker, size based on vessel_count
    for _, row in port_data.iterrows():
        folium.CircleMarker(
            location=[row['Latitude'], row['Longitude']],
            radius=3 + row['vessel_count'] ** 0.5,  # for non-linear scaling
            color='blue',
            fill=True,
            fill_opacity=0.6,
            popup=f"Port Cluster: {row['port_cluster']}<br>Vessels: {row['vessel_count']}",
        ).add_to(map)

    # Optionally showing noise points (in red)
    for _, row in df[df['port_cluster'] == -1].iterrows():
        folium.CircleMarker(
            location=[row['Latitude'], row['Longitude']],
            radius=2,
            color='red',
            fill=True,
            fill_opacity=0.5,
        ).add_to(map)

    log.info('Saving and opening ports map.')
    map.save('port_clusters_map.html')
    
    # Opening in browser.
    webbrowser.open('port_clusters_map.html')

def main(filepath):
    df = read_data(filepath)
    df = cluster_vessels(df)
    df = get_port_clusters(df)
    port_data = calculate_port_sizes(df)
    visualize_ports(port_data, df)

if __name__ == '__main__':
    filepath = 'files/cleaned/cleaned-aisdk-2025-03-01.csv'
    main(filepath)
