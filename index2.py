import s3fs
import numpy as np
import os
import shutil
from datetime import datetime, timedelta
from netCDF4 import Dataset

# Definir limites de coordenadas de interesse
lon_min, lon_max = -43.7, -43
lat_min, lat_max = -23.2, -22.7

# Diretório de saída
output_directory = "C:/Users/lucas/OneDrive/Desktop/CEFET/TCC/Grid com Eventos/input/"

def clear_directory(directory):
    """Limpa o diretório de saída."""
    if os.path.exists(directory):
        shutil.rmtree(directory)  
        print(f"Diretório {directory} limpo.")
    os.makedirs(directory, exist_ok=True)  

def download_files(start_date, end_date):
    """Baixa os arquivos GLM para um intervalo de datas especificado e faz o crop por coordenadas."""
    current_date = start_date
    fs = s3fs.S3FileSystem(anon=True)

    while current_date <= end_date:
        year = current_date.year
        day_of_year = current_date.timetuple().tm_yday  # Dia do ano
        bucket_path = f'noaa-goes16/GLM-L2-LCFA/{year}/{day_of_year:03d}/'

        print(f"Buscando arquivos para {current_date.strftime('%Y-%m-%d')} (dia {day_of_year})")
        
        # Diretório de saída específico para o dia atual
        day_output_directory = os.path.join(output_directory, current_date.strftime('%Y-%m-%d'))
        clear_directory(day_output_directory)

        # Iterar pelas subpastas de cada hora (00 a 23)
        files = []
        for hour in range(24):
            hour_path = bucket_path + f"{hour:02d}/"  
            try:
                hourly_files = fs.ls(hour_path)
                files.extend(hourly_files)  
            except FileNotFoundError:
                print(f"Hora {hour:02d} não encontrada no bucket. Pulando...")

        print(f"Total de arquivos encontrados para {current_date.strftime('%Y-%m-%d')}: {len(files)}")

        # Baixar arquivos e filtrar por coordenadas
        for file in files:
            file_name = file.split('/')[-1]
            local_file_path = os.path.join(day_output_directory, file_name)

            print(f"Baixando: {file} para {local_file_path}")
            fs.get(file, local_file_path)

            # Realizar o filtro geográfico após o download
            filter_by_coordinates(local_file_path)

        print(f"Download e filtro para {current_date.strftime('%Y-%m-%d')} concluídos.")
        current_date += timedelta(days=1)

def filter_by_coordinates(file_path):
    """Filtra os eventos GLM de um arquivo NetCDF com base nas coordenadas fornecidas."""
    dataset = None
    try:
        # Abrir o arquivo NetCDF
        dataset = Dataset(file_path, 'r')

        # Ajuste as variáveis de latitude e longitude conforme a listagem no arquivo
        longitudes = dataset.variables['flash_lon'][:]
        latitudes = dataset.variables['flash_lat'][:]

        # Exibir valores mínimo e máximo das variáveis para debug
        print(f"Longitude mínima: {longitudes.min()}, máxima: {longitudes.max()}")
        print(f"Latitude mínima: {latitudes.min()}, máxima: {latitudes.max()}")

        # Filtrar eventos que estão dentro das coordenadas desejadas
        mask = (
            (longitudes >= lon_min) & (longitudes <= lon_max) &
            (latitudes >= lat_min) & (latitudes <= lat_max)
        )

        # Se não houver eventos dentro do filtro, remover o arquivo
        if np.sum(mask) == 0:
            print(f"Nenhum evento dentro do filtro encontrado em {file_path}. Removendo arquivo.")
            # Fechar o dataset antes de remover o arquivo
            dataset.close()
            os.remove(file_path)
        else:
            print(f"Eventos dentro do filtro encontrados no arquivo {file_path}.")
        
    except Exception as e:
        print(f"Erro ao filtrar o arquivo {file_path}: {e}")
    finally:
        # Certifique-se de fechar o dataset
        if dataset and not dataset.isopen():
            try:
                dataset.close()
            except:
                print(f"Erro ao fechar o arquivo {file_path}. Ignorando...")

# Exemplo de uso
start_date = datetime(2023, 11, 18)
end_date = datetime(2023, 11, 19)
download_files(start_date, end_date)
