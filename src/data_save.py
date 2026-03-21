
import pandas as pd
import os
import pathlib as Path
'''Clase para descargar los datos'''

class DataSave:

  def __init__(self, ruta: str):
    self.ruta = Path.Path(ruta)
    self.ruta.mkdir(parents = True, exist_ok=True)

# ─────────────────────────────────────────
# Guarda un solo DataFrame en Parquet.
# ─────────────────────────────────────────
  def guardar_uno(self, df: pd.DataFrame, name: str):
    filepath = self.ruta / f"{name}.parquet"
    df.to_parquet(filepath, index=False, engine="pyarrow")
    size_mb = filepath.stat().st_size / (1024 * 1024)
    print(f"✅ {name}.parquet guardado | Tamaño: {size_mb:.2f} MB")


# ─────────────────────────────────────────
# Guarda todas las tablas del Star Schema de una vez.
# ─────────────────────────────────────────
  def guardar_todo(self, tablas: dict) -> None:

    print("\n💾 Guardando Star Schema en Parquet...\n")
    for name, df in tablas.items():
        self.guardar_uno(df, name)
    print("\n🎉 Todos los archivos guardados exitosamente.")


# ─────────────────────────────────────────
# Carga un archivo Parquet desde la carpeta Gold.
# ─────────────────────────────────────────
  def cargar(self, name: str) -> pd.DataFrame:
      filepath = self.ruta / f"{name}.parquet"
      df = pd.read_parquet(filepath, engine="pyarrow")
      print(f"📂 {name}.parquet cargado | Filas: {len(df):,}")
      return df


# ─────────────────────────────────────────
# Carga todos los archivos Parquet de la carpeta Gold.
# ─────────────────────────────────────────
  def cargar_todo(self) -> dict:
      tablas = {}
      for filepath in self.ruta.glob("*.parquet"):
          name = filepath.stem
          tablas[name] = self.cargar(name)
      return tablas