import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import cartopy.crs as ccrs
import os
import numpy as np
from netCDF4 import Dataset

input_directory = "data/goes16/glm_files/2024-01-13"
date = input_directory.split("/")[-1]
area = [-45.05290312102409, -42.35676996062447, -23.801876626302175, -21.699774257353113] #Area de interesse

# Configuração do plot e mapa
fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()})
ax.set_extent( area ) 
ax.coastlines(resolution='50m')

files = [os.path.join(input_directory, f) for f in os.listdir(input_directory) if f.endswith('.nc')]

def update_frame(i):
    ax.clear()
    ax.set_extent( area )  
    ax.coastlines(resolution='50m')
    
    file_path = files[i]
    with Dataset(file_path, 'r') as dataset:
        lons = dataset.variables['flash_lon'][:]
        lats = dataset.variables['flash_lat'][:]

        ax.scatter(lons, lats, color='red', s=10, transform=ccrs.PlateCarree(), label=f"Frame {i+1}")
        ax.legend(loc='upper right')

    ax.set_title(f"Eventos GLM - " + date)

ani = FuncAnimation(fig, update_frame, frames=len(files), repeat=True)

ani.save("glm_animation.gif", writer='pillow', fps=10)

plt.show()
