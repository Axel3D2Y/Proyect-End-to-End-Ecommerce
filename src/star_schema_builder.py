import pandas as pd
import numpy as np
import pandera.pandas as pa


'''Clase que implementa el esquema tipo estrella'''
class StarSchemaBuilder:

  def __init__(self,
               df_customers, df_geolocation,df_order_items,
               df_order_payments,
               df_order_reviews, df_orders,df_products,
               df_sellers
               ):
    self.customers = df_customers
    self.geolocation = df_geolocation # se queda
    self.order_items = df_order_items # se queda
    self.order_payments = df_order_payments # se queda
    self.order_reviews = df_order_reviews
    self.orders = df_orders #  se queda
    self.products = df_products
    self.sellers = df_sellers # se queda

# ─────────────────────────────────────────
# DIMENSIÓN: CLIENTES
# ─────────────────────────────────────────
  def dim_customers(self) -> pd.DataFrame:
    dim = self.customers[
        ['customer_id',
         'customer_zip_code_prefix',
         'customer_city',
         'customer_state'
    ]].drop_duplicates()
    print(f"✅ Dim customers: {dim.shape}")
    return dim


# ─────────────────────────────────────────
# DIMENSIÓN: REVIEWS
# ─────────────────────────────────────────
  def dim_orders_reviews(self) -> pd.DataFrame:
      dim = self.order_reviews[[
        'review_id',
        'order_id',
        'review_score',
        'review_comment_message'
      ]].drop_duplicates()
      print(f"✅ Dim reviews: {dim.shape}") # Changed from products to reviews for accuracy
      return dim


# ─────────────────────────────────────────
# DIMENSIÓN: PRODUCTOS
# ─────────────────────────────────────────
  def dim_products(self) -> pd.DataFrame:
      dim = self.products[[
          "product_id",
          "product_category_name",
          "product_weight_g",
          "product_length_cm"
      ]].drop_duplicates()
      print(f"✅ Dim products: {dim.shape}")
      return dim

# ─────────────────────────────────────────
# DIMENSIÓN: VENDEDORES
# ─────────────────────────────────────────
  def dim_sellers(self) -> pd.DataFrame:
      dim = self.sellers[[
          "seller_id",
          "seller_zip_code_prefix",
          "seller_city",
          "seller_state"
      ]].drop_duplicates()
      print(f"✅ Dim sellers: {dim.shape}")
      return dim


# ─────────────────────────────────────────
# DIMENSIÓN: TIEMPO
# ─────────────────────────────────────────
  def dim_time(self) -> pd.DataFrame:
      df = self.orders[["order_purchase_timestamp"]].copy()
      df["purchase_date"] = pd.to_datetime(df["order_purchase_timestamp"])

      dim = pd.DataFrame({
          "date":      df["purchase_date"],
          "year":      df["purchase_date"].dt.year,
          "month":     df["purchase_date"].dt.month,
          "day":       df["purchase_date"].dt.day,
          "weekday":   df["purchase_date"].dt.day_name(),
          "quarter":   df["purchase_date"].dt.quarter
      }).drop_duplicates()
      print(f"✅ Dim Time: {dim.shape}")
      return dim

# ─────────────────────────────────────────
# TABLA DE HECHOS: VENTAS
# ─────────────────────────────────────────
  def fact_sales(self) -> pd.DataFrame:
      # Unir órdenes con items
      fact = self.orders.merge(self.order_items, on="order_id", how="inner")
      # Unir con pagos
      fact = fact.merge(
          self.order_payments[["order_id", "payment_value"]],
          on="order_id", how="left"
      )
      # Seleccionar columnas relevantes
      fact = fact[[
          "order_id",
          "customer_id",
          "product_id",
          "seller_id",
          "order_purchase_timestamp",
          "price",
          "freight_value",
          "payment_value"
      ]]

      print(f"✅ Fact Sales: {fact.shape}")
      return fact

  def build_all(self) -> dict:
      print("\n🌟 Construyendo Star Schema...\n")
      return {
          "fact_sales":         self.fact_sales(),
          "dim_geolocation":    self.geolocation,
          "dim_order_items":    self.order_items,
          "dim_order_payments": self.order_payments,
          "dim_orders":         self.orders,
          "dim_customers":      self.dim_customers(),
          "dim_orders_reviews": self.dim_orders_reviews(),
          "dim_products":       self.dim_products(),
          "dim_sellers":        self.dim_sellers(),
          "dim_time":           self.dim_time()

      }