import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
import urllib
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("backup_log.log")
    ]
)


def clean_numeric_columns(df, columns):
    for col in columns:
        df[col] = df[col].astype(str).str.replace(',', '').str.replace('.', '', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def convert_to_datetime(df, columns):
    for col in columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def create_db_engine():
    connection_string = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=Reporting;'
        'Trusted_Connection=yes;'
    )
    params = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    return engine


def test_connection(engine):
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logging.info("Tes koneksi berhasil!")
        return True
    except Exception as e:
        logging.error(f"Gagal terhubung ke database: {e}")
        return False


def get_sql_table_columns(engine, table_name):
    inspector = inspect(engine)
    columns = [column['name'] for column in inspector.get_columns(table_name)]
    return columns


def read_excel(file_path, columns):
    try:
        df = pd.read_excel(file_path, usecols=columns)
        logging.info(f"Berhasil membaca data dari {file_path}")
        return df
    except Exception as e:
        logging.error(f"Gagal membaca file Excel: {e}")
        sys.exit(1)


def save_to_sql(df, engine, table_name, dtype_mapping=None):
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False, dtype=dtype_mapping)
        logging.info(f"Backup data berhasil dilakukan ke tabel '{table_name}' di SQL Server!")
    except Exception as e:
        logging.error(f"Gagal menyimpan data ke SQL Server: {e}")
        sys.exit(1)


def main():
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

    file_excel = 'D:/OneDrive/ASM/DATA/2023/01. JANUARI 2023/20230102.xlsx'
    nama_file = os.path.splitext(os.path.basename(file_excel))[0]

    df = read_excel(file_excel, kolom_dipilih)
    logging.info("DataFrame setelah membaca Excel:")

    df_filtered = df[df['Status'] != 'EXP'].copy()
    df_filtered.insert(0, 'Periode', nama_file)
    df_filtered.dropna(how='all', inplace=True)

    numeric_columns = ['POKOK_AWAL', 'SALDO_POKOK', 'MaxOvd', 'Angsuran']
    df_filtered = clean_numeric_columns(df_filtered, numeric_columns)

    date_columns = ['Tgl_jt', 'Tgl_Cair', 'Tgl_Tarik', 'Tgl_Bayar', 'TglDO', 'Tgl_Proses']
    df_filtered = convert_to_datetime(df_filtered, date_columns)

    logging.info("DataFrame setelah pembersihan kolom numerik dan konversi datetime:")

    kolom_tabel_sql = get_sql_table_columns(engine, 'AZ_Daily')

    df_filtered.rename(columns=column_mapping, inplace=True)

    save_to_sql(df_filtered, engine, 'AZ_Daily')


if __name__ == "__main__":
    main()