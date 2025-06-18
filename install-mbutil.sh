#!/bin/bash
# mb-util installation and tile extraction utility
# Since mb-util might not be available via npm, we'll create our own tile extractor

# Install Python version of mb-util
pip3 install mbutil

# Create a custom tile extractor script
cat > /usr/local/bin/extract-mbtiles << 'EOF'
#!/usr/bin/env python3
"""
Custom MBTiles to directory extractor
Extracts PBF tiles from MBTiles files
"""
import sqlite3
import os
import sys
import argparse
from pathlib import Path

def extract_mbtiles(mbtiles_path, output_dir):
    """Extract tiles from MBTiles file to directory structure"""
    
    if not os.path.exists(mbtiles_path):
        print(f"Error: MBTiles file {mbtiles_path} not found")
        return False
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Connect to MBTiles database
        conn = sqlite3.connect(mbtiles_path)
        cursor = conn.cursor()
        
        # Get tile count
        cursor.execute("SELECT COUNT(*) FROM tiles")
        total_tiles = cursor.fetchone()[0]
        print(f"Extracting {total_tiles} tiles...")
        
        # Extract tiles
        cursor.execute("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles")
        
        extracted = 0
        for row in cursor.fetchall():
            zoom, col, row_tms, tile_data = row
            
            # Convert TMS row to XYZ row (flip Y coordinate)
            row_xyz = (2 ** zoom) - 1 - row_tms
            
            # Create directory structure
            tile_dir = os.path.join(output_dir, str(zoom), str(col))
            os.makedirs(tile_dir, exist_ok=True)
            
            # Write tile file
            tile_path = os.path.join(tile_dir, f"{row_xyz}.pbf")
            with open(tile_path, 'wb') as f:
                f.write(tile_data)
            
            extracted += 1
            if extracted % 1000 == 0:
                print(f"Extracted {extracted}/{total_tiles} tiles...")
        
        conn.close()
        print(f"Successfully extracted {extracted} tiles to {output_dir}")
        return True
        
    except Exception as e:
        print(f"Error extracting tiles: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Extract PBF tiles from MBTiles')
    parser.add_argument('mbtiles', help='Input MBTiles file')
    parser.add_argument('output_dir', help='Output directory')
    
    args = parser.parse_args()
    
    success = extract_mbtiles(args.mbtiles, args.output_dir)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
EOF

chmod +x /usr/local/bin/extract-mbtiles

echo "MBTiles extractor installed successfully"
