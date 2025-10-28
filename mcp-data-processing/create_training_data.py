import pandas as pd
import json
import re
from pathlib import Path

# Metadata untuk memproses file
FILE_METADATA = [
    {"filename": "Churn_Checkout.csv", "type": "churn", "product_name": "CHECKOUT"},
    {"filename": "Churn_Direct API.csv", "type": "churn", "product_name": "DIRECT API"},
    {"filename": "Churn_Plugin.csv", "type": "churn", "product_name": "PLUGIN"},
    {"filename": "Profit_Checkout.csv", "type": "profit", "product_name": "CHECKOUT"},
    {"filename": "Profit_Direct API.csv", "type": "profit", "product_name": "DIRECT API"},
    {"filename": "Profit_Plugin_trx _4months.csv", "type": "profit", "product_name": "PLUGIN"}
]

OUTPUT_FILENAME = "mcp_training_data.json"

def clean_number(value):
    """Membersihkan string angka dengan koma sebagai pemisah ribuan dan mengkonversinya menjadi integer."""
    if isinstance(value, str):
        # Hapus tanda kutip ganda dan koma
        value = re.sub(r'[",]', '', value)
        return int(value) if value.isdigit() else 0
    return int(value) if pd.notna(value) else 0

def create_mcp_training_data(file_metadata: list, output_filename: str):
    """
    Memuat, mentransformasi, dan menggabungkan semua data CSV menjadi format panjang (long format).
    Data yang dihasilkan disimpan sebagai file JSON.
    """
    all_dataframes = []

    for meta in file_metadata:
        filename = meta["filename"]
        data_type = meta["type"]
        
        try:
            # Baca CSV
            df = pd.read_csv(filename, header=0, dtype={'brand_id': str, 'Product Category': str})
        except Exception as e:
            print(f"Gagal memproses file {filename}. Error: {e}")
            continue
        
        # Kolom TPT dan TPV yang ada di data
        tpt_cols = [col for col in df.columns if col.startswith('tpt_')]
        tpv_cols = [col for col in df.columns if col.startswith('tpv_')]
        
        df_long = pd.DataFrame()
        
        # Lakukan iterasi untuk menggabungkan TPT dan TPV
        # Menggunakan kolom tpt_ untuk mendapatkan daftar bulan
        for col_tpt in tpt_cols:
            col_tpv = col_tpt.replace('tpt_', 'tpv_')
            
            if col_tpv in tpv_cols:
                # Ambil nama bulan dari kolom (misal: tpt_sep_2025 -> sep_2025)
                month_year_str = col_tpt.replace('tpt_', '')
                # Konversi menjadi format YYYY-MM yang lebih standar (misal: sep_2025 -> 2025-09).
                # Karena data Anda sudah memiliki format 'sep_2025', kita akan konversi formatnya menjadi YYYY-MM
                # dengan asumsi nama bulan adalah bulan singkat bahasa Inggris.
                try:
                    month_obj = pd.to_datetime(month_year_str, format='%b_%Y', errors='coerce')
                    month_str = month_obj.strftime('%Y-%m')
                except:
                    # Jika gagal parsing, gunakan format aslinya dengan '-'
                    month_str = month_year_str.replace('_', '-')

                # Buat DataFrame sementara untuk bulan ini
                df_temp = df[['brand_id', 'Product Category', col_tpt, col_tpv]].copy()
                df_temp['month'] = month_str
                df_temp = df_temp.rename(columns={
                    'Product Category': 'product', 
                    col_tpt: 'tpt', 
                    col_tpv: 'tpv'
                })
                
                df_long = pd.concat([df_long, df_temp], ignore_index=True)

        # Pembersihan dan Konversi Angka
        df_long['tpt'] = df_long['tpt'].apply(clean_number)
        df_long['tpv'] = df_long['tpv'].apply(clean_number)
        
        # Tambahkan kolom 'type'
        df_long['type'] = data_type
        
        # Filter baris dengan aktivitas (tpt atau tpv > 0)
        df_long = df_long[(df_long['tpt'] > 0) | (df_long['tpv'] > 0)].reset_index(drop=True)
        
        all_dataframes.append(df_long)

    if not all_dataframes:
        return "Gagal membuat data training. Tidak ada data yang berhasil diproses."

    final_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Konversi ke JSON (list of dictionaries)
    final_data_list = final_df.to_dict('records')

    # Simpan ke file JSON
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(final_data_list, f, indent=4)

    return f"Data training berhasil dibuat dan disimpan di **{output_filename}** dengan total **{len(final_data_list)}** records. JSON ini siap digunakan untuk FastMCP."

# Jalankan fungsi untuk menghasilkan file JSON
print(create_mcp_training_data(FILE_METADATA, OUTPUT_FILENAME))