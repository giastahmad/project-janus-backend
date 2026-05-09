import os
from etl.extract import extract_data
from etl.transform import transform_shopee, transform_tokopedia
from etl.load import load_data_warehouse

def run_etl_pipeline(file_path, platform):

    print(f"\n{'='*50}")
    print(f"🚀 STARTING AN ETL PIPELINE FOR: {platform.upper()}")
    print(f"📁 File: {file_path}")
    print(f"{'='*50}")
                                                                   
    try:
        # ==========================================
        # 1. EXTRACT
        # ==========================================
        print("\n[1/3] EXTRACT: Reading data from source...")
        df_raw = extract_data(file_path, platform)
        print(f"      -> Successfully extracted {len(df_raw)} rows of raw data.")

        # ==========================================
        # 2. TRANSFORM
        # ==========================================
        print("\n[2/3] TRANSFORM: Cleaning and standardizing data...")
        if platform.lower() == 'shopee':
            df_clean = transform_shopee(df_raw)
        elif platform.lower() == 'tokopedia':
            df_clean = transform_tokopedia(df_raw)
        else:
            raise ValueError(f"Transformation function for platform '{platform}' is not available.")
        
        print(f"      -> Transformation completed. {len(df_clean)} rows are ready for loading.")

        # ==========================================
        # 3. LOAD
        # ==========================================
        print("\n[3/3] LOAD: Inserting data into Data Warehouse (MySQL)...")
        load_data_warehouse(df_clean)

        print(f"\n✅ ETL PROCESS FOR {platform.upper()} COMPLETED SUCCESSFULLY!")

    except Exception as e:
        print(f"\n❌ ERROR IN ETL PIPELINE: {e}")


if __name__ == "__main__":
    
    # 1. Data Shopee
    file_shopee = 'data\\shopee apr 2026.xlsx' 
    if os.path.exists(file_shopee):
        run_etl_pipeline(file_shopee, 'shopee')
    else:
        print(f"[SKIP] File {file_shopee} not found.")
        
    # 2. Data Tokopedia
    file_tokopedia = ''
    if os.path.exists(file_tokopedia):
        run_etl_pipeline(file_tokopedia, 'tokopedia')
    else:
        print(f"[SKIP] File {file_tokopedia} not found.")