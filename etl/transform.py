import pandas as pd
import re

COLUMN_MAPPING = {
    "order_key": ["order_id", "nomor_pesanan", "No. Pesanan", "Order ID", "no_pesanan"],
    "status" : ["Status Pesanan", "Order Status", "status_pesanan", "Status"],
    "date" : ["Waktu Pesanan Dibuat", "Created Time", "Order Created"],
    "payment_method" : ["Metode Pembayaran", "Payment Method"],
    "SKU" : ["SKU Induk", "Seller SKU", "sku"],
    "variant" : ["Nama Variasi", "Variation"],
    "price" : ["Harga Awal", "SKU Unit Original Price", "Unit Price"],
    "price_after_discount" : ["Harga Setelah Diskon", "Price After Discount"],
    "quantity" : ["Jumlah", "Quantity"],
    "total_amount" : ["Total Pembayaran", "Total payment", "Order Amount"],
    "province" : ["Provinsi", "Province"],
    "city" : ["Kota/Kabupaten", "Regency and City", "City"]
}

REQUIRED_COLUMNS = [
    "order_key",
    "status",
    "date",
    "payment_method",
    "SKU",
    "variant",
    "price",
    "price_after_discount",
    "quantity",
    "total_amount",
    "province",
    "city"
]

PAYMENT_METHOD = {
    "COD" : [["cod (bayar di tempat)", "cod", "bayar di tempat (cod)", "bayar di tempat", "cash"],"MANUAL"],
    "PAYLATER" : [["spaylater", "gopay later", "paylater", "later"], "LEVERAGE"],
    "E-WALLET" : [["shopeepay", "dana", "gopay", "ovo", "linkaja", "ewallet", "e wallet"], "LIQUID"],
    "BANK_TRANSFER" : [["seabank", "jago", "transfer bank", "online payment", "qris", "bank transfer", "bank", "seabank bayar instan"], "LIQUID"],
    "CREDIT_CARD" : [["kartu kredit/debit", "credit card", "debit", "cicilan kartu kredit"], "LEVERAGE"],
    "STORE_BALANCE" : [["saldo penjual", "tiktok shop balance", "balance", "saldo shopeepay"], "LIQUID"],
    "OFFLIBE_AGENT" : [["mitra shopee"], "MANUAL"]
}

COLOR_MAPPING = {
    "ROSEGOLD" : "ROSE GOLD",
    "COKLATUA/COKSU(NOTE)" : "COKELAT TUA",
    "COKLAT TUA" : "COKELAT TUA",
    "COKSU/COKLATUA(NOTE)" : "COKELAT SUSU",
    "COKSU DK" : "COKELAT SUSU",
    "COKLAT SUSU" : "COKELAT SUSU",
    "BIRUMUDA/DENIM(NOTE)" : "BIRU MUDA",
    "DENIM/BIRUMUDA(NOTE)" : "DENIM",
    "MAROON DK" : "MAROON",
    "DUSTY ROSE" : "DUSTYPINK",
    "LAVENDER/LILAC(NOTE)" : "LAVENDER",
    "LAVENDER/LILAC" : "LAVENDER",
    "SILVER DK" : "SILVER",
    "PERAK" : "SILVER",
    "ROSEGOL DK" : "ROSE GOLD",
    "NAVY DK" : "NAVY",
}

MUSLIM = [
    "JASMINE",
    "M UNICORN",
    "AMEERA",
    "AERA LONG DRESS",
    "M ADHEYYA"
]

def map_columns(df):
  mapping_flat = {
      opt.lower(): standard
      for standard, options in COLUMN_MAPPING.items()
      for opt in options
  }

  new_columns = {
      col:mapping_flat[col.lower()]
      for col in df.columns
      if col.lower() in mapping_flat
  }

  return df.rename(columns=new_columns)

def map_data_payment(df):
  df['payment_method'] = df['payment_method'].str.lower().str.strip()

  mapping_flat = {
      opt: standard
      for standard, data in PAYMENT_METHOD.items()
      for opt in data[0]
  }

  mapping_category = {
      standard: data[1]
      for standard, data in PAYMENT_METHOD.items()
  }

  df['payment_method'] = df['payment_method'].map(mapping_flat)
  df['payment_category'] = df['payment_method'].map(mapping_category)

  return df

def extract_size(df):
  match = re.search(r'(\d+)(?:\s*-\s*(\d+))?', df)

  if match:
    num1 = int(match.group(1))
    num2 = match.group(2)

    if num2:
      num2 = int(num2)

      if num1 % 2 != 0:
        return f"{num1}-{num2} Tahun"
      else:
        return f"{num2}-{num2+1} Tahun"
    else:
      if num1 % 2 == 0:
        return f"{num1-1}-{num1} Tahun"
      else:
        return f"{num1}-{num1+1} Tahun"
  else:
    return None

def transform_shopee(df):
    df_standard = map_columns(df)
    df_standard = df_standard[REQUIRED_COLUMNS]
    
    df_standard = df_standard[df_standard['status'].str.lower() == 'selesai']

    df_standard['date'] = pd.to_datetime(df_standard['date']).dt.date
    
    df_standard = map_data_payment(df_standard)
    df_standard = df_standard[df_standard['SKU'].notna()]
    df_standard['SKU'] = df_standard['SKU'].str.upper()
    
    df_standard['color'] = df_standard['variant'].str.split(',').str[0].str.upper()
    df_standard['color'] = df_standard['color'].replace(COLOR_MAPPING)
    
    df_standard['size'] = df_standard['variant'].str.split(',').str[1].str.upper().apply(extract_size)
    
    df_standard['price'] = df_standard['price'] * 1000
    df_standard['price_after_discount'] = df_standard['price_after_discount'] * 1000
    df_standard['discount'] = df_standard['price'] - df_standard['price_after_discount']
    
    df_standard['total_amount'] = df_standard['total_amount'] * 1000
    df_standard['line_total'] = df_standard['price_after_discount'] * df_standard['quantity']
    order_totals = df_standard.groupby('order_key')['line_total'].transform('sum')
    
    df_standard['weight'] = df_standard["line_total"] / order_totals
    df_standard['total_amount'] = df_standard['total_amount'] * df_standard['weight']
    df_standard['total_amount'] = df_standard['total_amount'].round(0)
    
    df_standard['platform'] = "Shopee"
    
    df_standard['is_muslim_fashion'] = df_standard['SKU'].isin(MUSLIM)
    
    df_standard = df_standard.drop(columns=['weight', 'line_total', 'price_after_discount', 'variant'])
    
    return df_standard

def transform_tokopedia(df):
    # to be implemented
    return df