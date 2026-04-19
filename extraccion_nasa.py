import requests
import pandas as pd
import os

def extraccion_limpieza(latitud, longitud, fecha_inicio, fecha_fin):
    """" Extracción de datos de NASA POWER, los convierte a DataFrame y limpia los valores nulos"""
    url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    parametros :{
        "parameters": "ALLSKY_SFC_SW_DWN, T2M, RH2M", #Radiación, Temperatura, Humedad
        "community": "RE", #Renewable Energy
        "longitude": longitud,
        "latitude": latitud,
        "start": fecha_inicio,
        "end": fecha_fin,
        "format": "JSON"
    }

    respuesta = requests.get(url=parametros)

    if respuesta.status_code == 200:
        datos_json = respuesta.jason()

        #1. Se extrae solo la parte de los datos e ignoramos loss metadatos
        metricas = datos_json['properties']['parameter']

        #2. Se convierte el diccionario a un DataFrame de pandas
        df = pd.DataFrame(metricas)

        #3. El índice actual son las fechas (como '20230101') y se pasan a columnas
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'Fecha'}, inplace=True)

        #4. Formatear la columna de 'Fecha' para que python la entienda como teimpo real
        df['Fecha'] = pd.to_datetime(df['Fecha'], format = '%Y%m%d')

        #5. Se limpian los datos reemplazando -999.0 por NaN
        df.replace(-999.0, pd.NA, inplace=True)

        #6. Interpolar, si falta un día, se promedia con el dia anterior y el siguiente
        df.interpolate(method='linear', inplace=True)

        print("¡Datos transformados y limpios exitosamente!")
        return df
    else:
        print(f"Error en la API: {respuesta.status_code}")
        return None


if __name__ == "__main__":
    LAT_BOGOTA = 4.6097
    LON_BOGOTA = -74.0817
    INICIO = "20230101"
    FIN = "20231231"

    # Ejecutamos nuestra nueva función
    df_clima = extraer_y_limpiar_datos(LAT_BOGOTA, LON_BOGOTA, INICIO, FIN)
    
    if df_clima is not None:
        # Mostramos las primeras 5 filas para verificar
        print("\nPrimeros 5 registros:")
        print(df_clima.head())
        
        # Guardamos los datos en la carpeta 'data/' (creándola si no existe)
        os.makedirs("data", exist_ok=True)
        ruta_archivo = "data/dataset_bogota_2023.csv"
        df_clima.to_csv(ruta_archivo, index=False)
        print(f"\n✅ Archivo guardado correctamente en: {ruta_archivo}")
 