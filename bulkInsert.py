import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
import urllib
import sys
import logging
from insert import read_excel, clean_numeric_columns, convert_to_datetime, create_db_engine, test_connection, get_sql_table_columns, save_to_sql

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("backup_log.log")
    ]
)


def process_files_in_folder(folder_path):
    engine = create_db_engine()
    if not test_connection(engine):
        logging.error("Akhiri skrip karena koneksi ke database gagal.")
        sys.exit(1)
    kolom_dipilih = [
        'BranchID', 'AgreementNo', 'CustomerID', 'Nama', 'Model', 'Tenor', 'Ang_ke',
        'POKOK_AWAL', 'SALDO_POKOK', 'OVD', 'Status', 'Tgl_jt', 'Tgl_Cair', 'ProductId', 'StatusWo',
        'StatusAsset', 'Tgl_Tarik', 'Tgl_Bayar', 'TglDO', 'Tgl_Proses', 'MaxOvd', 'POS_ID', 'ApplicationPriority',
        'BAname', 'CMO', 'Angsuran', 'ProfID', 'SourceApplication'
    ]
    column_mapping = {
        'BranchID': 'BranchID',
        'AgreementNo': 'AgreementNo',
        'CustomerID': 'CustomerID',
        'Nama': 'Nama',
        'Model': 'Model',
        'Tenor': 'Tenor',
        'Ang_ke': 'Ang_ke',
        'POKOK_AWAL': 'Pokok_Awal',
        'SALDO_POKOK': 'Saldo_Pokok',
        'OVD': 'OVD',
        'Status': 'Status',
        'Tgl_jt': 'Tgl_Jt',
        'Tgl_Cair': 'Tgl_Cair',
        'ProductId': 'ProductId',
        'StatusWo': 'StatusWo',
        'StatusAsset': 'StatusAsset',
        'Tgl_Tarik': 'Tgl_Tarik',
        'Tgl_Bayar': 'Tgl_Bayar',
        'TglDO': 'TglDO',
        'Tgl_Proses': 'Tgl_Proses',
        'MaxOvd': 'Max_OVD',
        'POS_ID': 'POS_ID',
        'ApplicationPriority': 'ApplicationPriority',
        'BAname': 'BAName',
        'CMO': 'CMO',
        'Angsuran': 'Angsuran',
        'ProfID': 'Profession',
        'SourceApplication': 'SourceApplication'
    }
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.xlsx'):
            file_path = os.path.join(folder_path, file_name)
            nama_file = os.path.splitext(file_name)[0]
            df = read_excel(file_path, kolom_dipilih)
            logging.info(f"DataFrame setelah membaca {file_name}:")

            df_filtered = df[df['Status'] != 'EXP'].copy()
            df_filtered.insert(0, 'Periode', nama_file)
            df_filtered.dropna(how='all', inplace=True)

            numeric_columns = ['POKOK_AWAL', 'SALDO_POKOK', 'MaxOvd', 'Angsuran']
            df_filtered = clean_numeric_columns(df_filtered, numeric_columns)

            date_columns = ['Tgl_jt', 'Tgl_Cair', 'Tgl_Tarik', 'Tgl_Bayar', 'TglDO', 'Tgl_Proses']
            df_filtered = convert_to_datetime(df_filtered, date_columns)

            kolom_tabel_sql = get_sql_table_columns(engine, 'AZ_Daily')

            df_filtered.rename(columns=column_mapping, inplace=True)

            save_to_sql(df_filtered, engine, 'AZ_Daily')


def main():
    folder_path = 'D:/OneDrive/ASM/DATA/2023/02. FEBRUARI 2023'
    process_files_in_folder(folder_path)


if __name__ == "__main__":
    main()
