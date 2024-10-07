import numpy as np
from netCDF4 import Dataset
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import os

input_dir = 'C:/Users/lucas/OneDrive/Desktop/CEFET/TCC/Grid com Eventos/input/2023-11-18/'  

files = [f for f in os.listdir(input_dir) if f.endswith('.nc')]

all_flash_lat = []
all_flash_lon = []

for file in files:
    filepath = os.path.join(input_dir, file)
    dataset = Dataset(filepath, 'r')
    
    flash_lat = dataset.variables['flash_lat'][:]  
    flash_lon = dataset.variables['flash_lon'][:]  
    
    all_flash_lat.append(flash_lat)
    all_flash_lon.append(flash_lon)

all_flash_lat = np.concatenate(all_flash_lat)
all_flash_lon = np.concatenate(all_flash_lon)


lon_min, lon_max = -43.7, -43
lat_min, lat_max = -23.2, -22.7

num_divisions = 10  

lat_bins = np.linspace(lat_min, lat_max, num_divisions + 1)
lon_bins = np.linspace(lon_min, lon_max, num_divisions + 1)

counts, _, _ = np.histogram2d(all_flash_lat, all_flash_lon, bins=[lat_bins, lon_bins])

norm_counts = counts / counts.max()

fig = plt.figure(figsize=(6, 6), dpi=200)
ax = plt.axes(projection=ccrs.Mercator())
ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())

ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS)

cmap = plt.get_cmap('coolwarm') 

for i in range(num_divisions):
    for j in range(num_divisions):
        color = cmap(norm_counts[i, j]) 
        ax.add_patch(plt.Rectangle((lon_bins[j], lat_bins[i]), lon_bins[j+1]-lon_bins[j], lat_bins[i+1]-lat_bins[i],
                                   edgecolor='black', facecolor=color, transform=ccrs.PlateCarree()))

        ax.text((lon_bins[j]+lon_bins[j+1])/2, (lat_bins[i]+lat_bins[i+1])/2, int(counts[i, j]),
                ha='center', va='center', transform=ccrs.PlateCarree(), fontsize=8, color='black')

ax.scatter(all_flash_lon, all_flash_lat, color='yellow', s=1, transform=ccrs.PlateCarree())

plt.show()
