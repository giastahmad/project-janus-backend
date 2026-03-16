import pandas as pd
import json
import glob
import os
from config import SessionLocal
from models import LocationDimension

def seed_location_dimension():
    data_provinsi = pd.read_json('data/provinsi.json', orient='index')
    data_provinsi = data_provinsi.reset_index()
    data_provinsi.columns = ['kode_provinsi', 'province']
    data_provinsi['kode_provinsi'] = data_provinsi['kode_provinsi'].astype('str')
    
    data_kota = []
    files = glob.glob('data/kabupaten_kota/kab-*.json')

    for file in files:
        filename = os.path.basename(file)
        kode_prov = filename.split('-')[1].split('.')[0]

        with open(file) as f:
            data = json.load(f)
        
        for kode_kota, nama_kota in data.items():
            data_kota.append({
                "kode_provinsi": kode_prov,
                "kode_kota": kode_kota,
                "city" : nama_kota
            })

    df_kota = pd.DataFrame(data_kota)
    df_kota['kode_provinsi'] = df_kota['kode_provinsi'].astype('str')
    
    df_final = df_kota.merge(data_provinsi, on='kode_provinsi')
    df_final = df_final.drop(columns=['kode_provinsi', 'kode_kota'])
    
    db = SessionLocal()
    try:
        existing_locations = db.query(LocationDimension).count()
        if existing_locations != 0:
            print("Location Dimension already seeded, skip the process.")
            return

        records = df_final.to_dict(orient='records')
        
        db.bulk_insert_mappings(LocationDimension, records)
        db.commit()
        print("Location Dimension seeded successfully!")
    except Exception as e:
        db.rollback()
        print(f"Error seeding Location Dimension: {e}")
    finally:
        db.close()
        
        
if __name__ == "__main__":
    seed_location_dimension()