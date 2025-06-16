#!/usr/bin/env python3
"""
Humidity (RH2M) Data Processing Pipeline
Similar to precipitation workflow but for NASA POWER humidity data

This script:
1. Downloads NASA POWER RH2M (Relative Humidity at 2 Meters) data
2. Filters for land points only (excludes ocean areas)
3. Processes CSV data into monthly GeoJSON files with polygon grid cells
4. Generates MBTiles for web visualization using tippecanoe
5. Creates a complete tile server workflow

Workflow: CSV → Land Filtering → Monthly GeoJSON (Polygons) → Monthly MBTiles

Author: Generated for IITM Internship Project
Date: June 2025
"""

import xarray as xr
import pandas as pd
import numpy as np
import os
import time
import traceback
import subprocess
from pathlib import Path
from tqdm import tqdm
import json
from shapely.geometry import Polygon
import geopandas as gpd
from global_land_mask import globe

class HumidityProcessor:
    def __init__(self):
        # Configuration
        self.ZARR_URL = "s3://nasa-power/merra2/temporal/power_merra2_monthly_temporal_utc.zarr"
        self.VARIABLE = "RH2M"
        self.OUTPUT_DIR = "humidity_data_output"
        self.MBTILES_DIR = "humidity_mbtiles_output"
        
        # Create directories
        for directory in [self.OUTPUT_DIR, self.MBTILES_DIR]:
            os.makedirs(directory, exist_ok=True)
        
        # Date range (2022-01 to 2025-05)
        self.start_year = 2022
        self.end_year = 2025
        self.end_month = 5
        
        print("🌡️ Humidity (RH2M) Data Processing Pipeline")
        print("📅 Processing data from 2022-01 to 2025-05")
        print("=" * 60)

    def time_filter(self, t):
        """Filter for dates from January 2022 to May 2025"""
        start_condition = (t.year > 2021) | ((t.year == 2022) & (t.month >= 1))
        end_condition = (t.year < 2025) | ((t.year == 2025) & (t.month <= 5))
        return start_condition & end_condition

    def download_humidity_data(self):
        """Download humidity data from NASA POWER Zarr store"""
        print("\n[STEP 1] Downloading NASA POWER RH2M Data")
        print("-" * 40)
        
        try:
            print("📡 Opening remote Zarr store...")
            ds = xr.open_dataset(
                self.ZARR_URL,
                engine="zarr",
                backend_kwargs={
                    "consolidated": True,
                    "storage_options": {"anon": True},
                },
            )
            print("✅ Remote Zarr store opened successfully")
            
            # Filter time
            print("📅 Filtering time range...")
            all_times = pd.to_datetime(ds.time.values)
            filtered_times = all_times[self.time_filter(all_times)]
            
            if filtered_times.empty:
                raise ValueError("No matching data found for specified date range")
            
            print(f"✅ Found {len(filtered_times)} monthly timestamps")
            
            # Extract data
            print("💾 Extracting humidity data...")
            da_subset = ds[self.VARIABLE].sel(time=filtered_times)
            df = da_subset.to_dataframe().reset_index()
            
            # Clean data
            print("🧹 Cleaning data (removing NaNs)...")
            before = len(df)
            df = df.dropna(subset=[self.VARIABLE])
            after = len(df)
            print(f"✅ Cleaned data: {before} → {after} rows ({before-after} NaNs removed)")
            
            # Save main CSV
            main_csv = os.path.join(self.OUTPUT_DIR, f"{self.VARIABLE}_monthly_2022_2025.csv")
            df.to_csv(main_csv, index=False)
            print(f"💾 Saved main dataset: {main_csv}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error downloading data: {e}")
            traceback.print_exc()
            return None

    def split_monthly_data(self, df):
        """Split the main dataframe into monthly CSV files"""
        print("\n[STEP 2] Splitting into Monthly Files")
        print("-" * 40)
        
        monthly_files = []
        
        # Group by year and month
        df['year'] = pd.to_datetime(df['time']).dt.year
        df['month'] = pd.to_datetime(df['time']).dt.month
        
        for (year, month), group in tqdm(df.groupby(['year', 'month']), desc="Creating monthly files"):
            filename = f"humidity_{month:02d}_{year}.csv"
            filepath = os.path.join(self.OUTPUT_DIR, filename)
            
            # Save monthly data
            monthly_data = group[['time', 'lat', 'lon', self.VARIABLE]].copy()
            monthly_data.to_csv(filepath, index=False)
            monthly_files.append(filepath)
        
        print(f"✅ Created {len(monthly_files)} monthly CSV files")
        return monthly_files

    def csv_to_geojson(self, csv_file, output_geojson):
        """Convert CSV humidity data to GeoJSON with polygon grid cells (land only)"""
        try:
            # Read CSV
            df = pd.read_csv(csv_file)
            
            if df.empty:
                print(f"⚠️ Empty CSV file: {csv_file}")
                return False
            
            print(f"📊 Initial data points: {len(df)}")
            
            # Filter for land points only
            print("🌍 Filtering for land points only...")
            land_mask = globe.is_land(df['lat'].values, df['lon'].values)
            df_land = df[land_mask].copy()
            
            if df_land.empty:
                print(f"⚠️ No land points found in {csv_file}")
                return False
            
            print(f"🏞️ Land points: {len(df_land)} ({len(df_land)/len(df)*100:.1f}% of total)")
            
            # Get unique coordinates to determine grid resolution
            unique_lats = sorted(df_land['lat'].unique())
            unique_lons = sorted(df_land['lon'].unique())
            
            # Calculate grid cell size (assuming regular grid)
            if len(unique_lats) > 1:
                lat_res = abs(unique_lats[1] - unique_lats[0])
            else:
                lat_res = 0.5  # Default resolution
                
            if len(unique_lons) > 1:
                lon_res = abs(unique_lons[1] - unique_lons[0])
            else:
                lon_res = 0.625  # Default resolution
            
            print(f"📐 Grid resolution: {lat_res}° lat × {lon_res}° lon")
            
            # Create polygon features for land points only
            features = []
            for _, row in df_land.iterrows():
                lat = row['lat']
                lon = row['lon']
                humidity = row[self.VARIABLE]
                
                # Skip NaN values
                if pd.isna(humidity):
                    continue
                
                # Create polygon for grid cell (centered on lat/lon)
                half_lat = lat_res / 2
                half_lon = lon_res / 2
                
                # Define polygon coordinates (rectangle)
                coordinates = [[
                    [lon - half_lon, lat - half_lat],  # SW corner
                    [lon + half_lon, lat - half_lat],  # SE corner
                    [lon + half_lon, lat + half_lat],  # NE corner
                    [lon - half_lon, lat + half_lat],  # NW corner
                    [lon - half_lon, lat - half_lat]   # Close polygon
                ]]
                
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": coordinates
                    },
                    "properties": {
                        "humidity": humidity,
                        "time": row['time'],
                        "lat": lat,
                        "lon": lon
                    }
                }
                features.append(feature)
            
            geojson = {
                "type": "FeatureCollection",
                "features": features
            }
            
            # Save GeoJSON
            with open(output_geojson, 'w') as f:
                json.dump(geojson, f)
            
            print(f"✅ Created {len(features)} land polygon features")
            return True
            
        except Exception as e:
            print(f"❌ Error creating land-filtered GeoJSON polygons for {csv_file}: {e}")
            return False

    def geojson_to_mbtiles_tippecanoe(self, geojson_file, output_mbtiles):
        """Convert GeoJSON polygons to MBTiles using tippecanoe"""
        try:
            # Use tippecanoe to convert GeoJSON to MBTiles (optimized for polygons)
            cmd = [
                'tippecanoe',
                '-o', output_mbtiles,
                '-z', '10',  # max zoom
                '-Z', '0',   # min zoom
                '--no-feature-limit',  # No limit on features per tile
                '--no-tile-size-limit',  # No limit on tile size
                '-B0',  # No tile buffer (good for grid cells)
                '--drop-densest-as-needed',
                '--extend-zooms-if-still-dropping',
                '--force',  # Overwrite existing files
                geojson_file
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                return True
            else:
                print(f"⚠️ Tippecanoe failed: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Error converting GeoJSON polygons to MBTiles: {e}")
            return False

    def create_geojsons(self, monthly_files):
        """Create GeoJSON files from monthly CSV files"""
        print("\n[STEP 3] Creating GeoJSON Files")
        print("-" * 40)
        
        geojson_files = []
        
        for csv_file in tqdm(monthly_files, desc="Creating GeoJSONs"):
            # Extract date from filename
            base_name = os.path.basename(csv_file).replace('.csv', '')
            geojson_file = os.path.join(self.OUTPUT_DIR, f"{base_name}_land.geojson")
            
            if self.csv_to_geojson(csv_file, geojson_file):
                geojson_files.append(geojson_file)
        
        print(f"✅ Created {len(geojson_files)} GeoJSON files")
        return geojson_files

    def create_mbtiles(self, geojson_files):
        """Create MBTiles from GeoJSON files using tippecanoe"""
        print("\n[STEP 4] Creating MBTiles from GeoJSON")
        print("-" * 40)
        
        mbtiles_files = []
        
        for geojson_file in tqdm(geojson_files, desc="Creating MBTiles"):
            # Extract base name
            base_name = os.path.basename(geojson_file).replace('_land.geojson', '')
            mbtiles_file = os.path.join(self.MBTILES_DIR, f"{base_name}_land.mbtiles")
            
            if self.geojson_to_mbtiles_tippecanoe(geojson_file, mbtiles_file):
                mbtiles_files.append(mbtiles_file)
                print(f"✅ Created: {base_name}.mbtiles")
            else:
                print(f"⚠️ Failed to create MBTiles for {base_name}")
        
        print(f"✅ Created {len(mbtiles_files)} MBTiles files")
        return mbtiles_files

    def create_tileserver_config(self, mbtiles_files):
        """Create tileserver-gl configuration for humidity tiles"""
        print("\n[STEP 5] Creating TileServer Configuration")
        print("-" * 40)
        
        config = {
            "options": {
                "paths": {
                    "root": "",
                    "mbtiles": f"./{self.MBTILES_DIR}"
                },
                "serveStaticMaps": True,
                "formatQuality": {
                    "jpeg": 90,
                    "webp": 90
                },
                "maxSize": 8192,
                "pbfAlias": "pbf"
            },
            "data": {}
        }
        
        # Add each MBTiles file to config
        for mbtiles_file in mbtiles_files:
            base_name = os.path.basename(mbtiles_file).replace('_land.mbtiles', '')
            config["data"][f"{base_name}_land"] = {"mbtiles": os.path.basename(mbtiles_file)}
        
        # Save config
        config_file = "humidity-tileserver-config.json"
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"✅ Created tileserver config: {config_file}")
        return config_file

    def create_web_viewer(self):
        """Create HTML viewer for humidity tiles"""
        html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Humidity Data Viewer</title>
    <script src='https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.js'></script>
    <link href='https://api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css' rel='stylesheet' />
    <style>
        body { margin: 0; padding: 0; font-family: Arial, sans-serif; }
        #map { position: absolute; top: 0; bottom: 0; width: 100%; }
        .controls {
            position: absolute;
            top: 10px;
            left: 10px;
            z-index: 1000;
            background: rgba(255, 255, 255, 0.95);
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 15px rgba(0,0,0,0.2);
            min-width: 300px;
        }
        .controls h3 {
            margin: 0 0 15px 0;
            color: #2c5282;
            font-size: 18px;
        }
        select, button {
            margin: 5px;
            padding: 8px 12px;
            border: 1px solid #cbd5e0;
            border-radius: 4px;
            font-size: 14px;
        }
        button {
            background: #3182ce;
            color: white;
            cursor: pointer;
            border: none;
        }
        button:hover {
            background: #2c5282;
        }
        .info {
            position: absolute;
            bottom: 10px;
            left: 10px;
            z-index: 1000;
            background: rgba(0, 0, 0, 0.85);
            color: white;
            padding: 12px;
            border-radius: 6px;
            font-family: monospace;
            font-size: 12px;
        }
        .legend {
            position: absolute;
            bottom: 10px;
            right: 10px;
            z-index: 1000;
            background: rgba(255, 255, 255, 0.95);
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 15px rgba(0,0,0,0.2);
        }
        .legend h4 {
            margin: 0 0 10px 0;
            font-size: 14px;
        }
        .color-bar {
            width: 200px;
            height: 20px;
            background: linear-gradient(to right, 
                #8B4513 0%, 
                #D2691E 25%, 
                #FFD700 50%, 
                #32CD32 75%, 
                #0000FF 100%);
            border: 1px solid #ccc;
            margin-bottom: 5px;
        }
        .legend-labels {
            display: flex;
            justify-content: space-between;
            font-size: 11px;
            color: #666;
        }
    </style>
</head>
<body>
    <div class="controls">
        <h3>🌡️ Humidity Data Viewer</h3>
        <div>
            <label for="yearSelect">Year:</label>
            <select id="yearSelect">
                <option value="2022">2022</option>
                <option value="2023">2023</option>
                <option value="2024" selected>2024</option>
                <option value="2025">2025</option>
            </select>
        </div>
        
        <div>
            <label for="monthSelect">Month:</label>
            <select id="monthSelect">
                <option value="01">January</option>
                <option value="02">February</option>
                <option value="03">March</option>
                <option value="04">April</option>
                <option value="05">May</option>
                <option value="06" selected>June</option>
                <option value="07">July</option>
                <option value="08">August</option>
                <option value="09">September</option>
                <option value="10">October</option>
                <option value="11">November</option>
                <option value="12">December</option>
            </select>
        </div>
        
        <button onclick="updateLayer()">Update Layer</button>
        <button onclick="toggleOpacity()">Toggle Opacity</button>
    </div>

    <div class="legend">
        <h4>Relative Humidity (%)</h4>
        <div class="color-bar"></div>
        <div class="legend-labels">
            <span>0%</span>
            <span>25%</span>
            <span>50%</span>
            <span>75%</span>
            <span>100%</span>
        </div>
    </div>

    <div class="info">
        <div>Tile Server: <span id="serverStatus">Checking...</span></div>
        <div>Current Layer: <span id="currentLayer">-</span></div>
        <div>Zoom Level: <span id="zoomLevel">-</span></div>
        <div>Data: NASA POWER RH2M (Monthly)</div>
    </div>

    <div id="map"></div>

    <script>
        // Initialize map
        const map = new mapboxgl.Map({
            container: 'map',
            style: {
                version: 8,
                sources: {},
                layers: [
                    {
                        id: 'background',
                        type: 'background',
                        paint: {
                            'background-color': '#f0f8ff'
                        }
                    }
                ]
            },
            center: [78.9629, 20.5937], // Center of India
            zoom: 4
        });

        let currentOpacity = 0.8;
        const tileServerUrl = 'http://localhost:8080';

        // Check if tile server is running
        function checkTileServer() {
            fetch(tileServerUrl)
                .then(response => {
                    if (response.ok) {
                        document.getElementById('serverStatus').textContent = 'Online ✓';
                        document.getElementById('serverStatus').style.color = '#48bb78';
                    } else {
                        throw new Error('Server responded with error');
                    }
                })
                .catch(error => {
                    document.getElementById('serverStatus').textContent = 'Offline ✗';
                    document.getElementById('serverStatus').style.color = '#e53e3e';
                });
        }

        // Update the humidity layer
        function updateLayer() {
            const year = document.getElementById('yearSelect').value;
            const month = document.getElementById('monthSelect').value;
            const layerId = `humidity_${month}_${year}_land`;
            
            // Remove existing humidity layer if it exists
            if (map.getLayer('humidity-layer')) {
                map.removeLayer('humidity-layer');
            }
            if (map.getSource('humidity-source')) {
                map.removeSource('humidity-source');
            }

            // Add new humidity layer
            map.addSource('humidity-source', {
                type: 'raster',
                tiles: [`${tileServerUrl}/data/${layerId}/{z}/{x}/{y}.png`],
                tileSize: 256,
                minzoom: 0,
                maxzoom: 18
            });

            map.addLayer({
                id: 'humidity-layer',
                type: 'raster',
                source: 'humidity-source',
                paint: {
                    'raster-opacity': currentOpacity
                }
            });

            document.getElementById('currentLayer').textContent = layerId;
        }

        // Toggle layer opacity
        function toggleOpacity() {
            currentOpacity = currentOpacity === 0.8 ? 0.4 : 0.8;
            if (map.getLayer('humidity-layer')) {
                map.setPaintProperty('humidity-layer', 'raster-opacity', currentOpacity);
            }
        }

        // Update zoom level display
        map.on('zoom', () => {
            document.getElementById('zoomLevel').textContent = Math.round(map.getZoom() * 100) / 100;
        });

        // Initialize
        map.on('load', () => {
            updateLayer();
            checkTileServer();
            setInterval(checkTileServer, 5000);
        });

        // Add navigation controls
        map.addControl(new mapboxgl.NavigationControl());
        map.addControl(new mapboxgl.FullscreenControl());
    </script>
</body>
</html>"""
        
        with open('humidity-viewer.html', 'w') as f:
            f.write(html_content)
        
        print("✅ Created web viewer: humidity-viewer.html")

    def run_complete_pipeline(self):
        """Run the complete humidity processing pipeline"""
        start_time = time.time()
        
        print("🚀 Starting Complete Humidity Processing Pipeline")
        print("=" * 60)
        
        # Step 1: Download data
        df = self.download_humidity_data()
        if df is None:
            print("❌ Pipeline failed at data download step")
            return
        
        # Step 2: Split into monthly files
        monthly_files = self.split_monthly_data(df)
        
        # Step 3: Create GeoJSON files
        geojson_files = self.create_geojsons(monthly_files)
        
        # Step 4: Create MBTiles
        mbtiles_files = self.create_mbtiles(geojson_files)
        
        # Step 5: Create tileserver config
        config_file = self.create_tileserver_config(mbtiles_files)
        
        # Step 6: Create web viewer
        self.create_web_viewer()
        
        # Summary
        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print("🎉 PIPELINE COMPLETE!")
        print(f"⏱️ Total execution time: {elapsed:.2f} seconds")
        print(f"📊 Processed {len(monthly_files)} monthly datasets")
        print(f"🗺️ Created {len(geojson_files)} GeoJSON files")
        print(f"📦 Created {len(mbtiles_files)} MBTiles files")
        print("\n📋 Next steps:")
        print("1. Start tileserver: tileserver-gl --config humidity-tileserver-config.json --port 8080")
        print("2. Open humidity-viewer.html in your browser")
        print("3. Explore humidity data interactively!")
        print("\n💡 Workflow: CSV → Land Filtering → GeoJSON (Polygons) → MBTiles (Land-only grid cells!)")

if __name__ == "__main__":
    processor = HumidityProcessor()
    processor.run_complete_pipeline()
