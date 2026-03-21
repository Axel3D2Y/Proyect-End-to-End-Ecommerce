'''Clase para descargar cargar los datos'''

import pandas as pd
import os
import pathlib as Path

class DataLoader:

  Olist_archivos = {
      "Ordenes":           "olist_orders_dataset.csv",
      "Objetos_ordenes":   "olist_order_items_dataset.csv",
      "clientes":          "olist_customers_dataset.csv",
      "productos":         "olist_products_dataset.csv",
      "vendedores":        "olist_sellers_dataset.csv",
      "pagos":             "olist_order_payments_dataset.csv",
      "reseñas":           "olist_order_reviews_dataset.csv",
      "geolocalizacion":   "olist_geolocation_dataset.csv",
    }


  '''Constructor para cargar los datos desde la ruta'''
  def __init__(self, ruta: str):
    self.ruta = Path.Path(ruta)
    self.dataframe = {}

  '''Cargar un archivo CSV por el  nombre del diccionario'''
  def carga_archivo(self, nombre: str) -> pd.DataFrame:
    nombre_archivo = self.Olist_archivos.get(nombre)

    if not nombre_archivo:
      raise ValueError(f'Dataset {nombre} no encontrado')

    ruta_archivo =  self.ruta / nombre_archivo
    print(f'Cargando archivo {nombre_archivo}')
    df = pd.read_csv(ruta_archivo)
    self.dataframe[nombre] = df
    return df

  def cargar_todo(self) -> dict:
    for nombre in self.Olist_archivos:
      self.carga_archivo(nombre)
    print(f"Se cargaron {len(self.dataframe)} datasets")
    return self.dataframe

  '''Resumen de los datos cargados'''
  def data_info(self):
    for name, df in self.dataframe.items():
      print(f"\n {name.upper()}")
      print(f"   Filas: {df.shape[0]:,} | Columnas: {df.shape[1]}")
      print(f"   Columnas: {list(df.columns)}")