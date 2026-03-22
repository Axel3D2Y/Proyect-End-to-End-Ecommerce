import pandas as pd
import numpy as np
import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check
from sklearn.impute import KNNImputer


'''Clase para realizar el procesamiento de los datos'''

class DataCleaner:
  '''Vamos a trabajar sobre una copia de los datos'''
  def __init__(self, df: pd.DataFrame):
    self.df = df.copy()

# ─────────────────────────────────────────
# Imputacion de datos usando Knn
# ─────────────────────────────────────────
  def imputacion_num(self, n_neighbors: int = 5 ) -> "DataCleaner":
    columnas_num = self.df.select_dtypes(include=[np.number]).columns.tolist()
    if not columnas_num:
      print('No hay columnas numericas')
      return self
    imputer = KNNImputer(n_neighbors= n_neighbors)
    self.df[columnas_num] = imputer.fit_transform(self.df[columnas_num])
    print(f'Columnas imputadas: {columnas_num}')
    return self

# ─────────────────────────────────────────
# Validacion de datos usando pandera
# ─────────────────────────────────────────
  def validacion_esquema(self, esquema: pa.DataFrameSchema) -> "DataCleaner":
    try:
      esquema.validate(self.df)
      print("Esquema validado correctamente")
    except pa.errors.SchemaError as e:
      print(f"Error de esquema: {e}")
    return self

  def df_limpio(self) -> pd.DataFrame:
    return self.df