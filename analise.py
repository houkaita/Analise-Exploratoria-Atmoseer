import xarray as xr

# Abra o arquivo NetCDF
file_path = 'data/goes16/glm_files/2023-01-13/OR_GLM-L2-LCFA_G16_s20230130000000_e20230130000200_c20230130000214.nc'
dataset = xr.open_dataset(file_path)

# Exiba as variáveis e dimensões do dataset
print(dataset)

# Se quiser ver apenas os nomes das variáveis
print("\nVariáveis do arquivo:")
print(list(dataset.data_vars))

# Se quiser ver as dimensões
print("\nDimensões do arquivo:")
print(list(dataset.dims))

# Fechar o dataset
dataset.close()
