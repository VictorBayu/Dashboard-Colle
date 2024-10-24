from imports import *
from source.column import *

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("backup_log.log")
    ]
)


def normalize_column_name(col_name):
    return re.sub(r'\s|_', '', col_name.strip().lower())


def get_dynamic_column(df_column, column_mapping):
    dynamic_column = {}
    df_column_normalized = {normalize_column_name(col):col for col in df_column}
    for key, value in column_mapping.items():
        key_processed = normalize_column_name(key)
        matched_col = df_column_normalized.get(key_processed, None)
        if matched_col:
            dynamic_column[matched_col] = value
    return dynamic_column


def clean_numeric_columns(df, columns):
    for col in columns:
        #df[col] = df[col].astype(str).str.replace(',', '').str.replace('.', '', regex=False)
        df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
        #df[col] = df[col].astype(str).str.replace('[^\d.]', '', regex=True)
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


# def read_excel(file_path, columns):
#     try:
#         df = pd.read_excel(file_path, usecols=columns)
#         logging.info(f"Berhasil membaca data dari {file_path}")
#         return df
#     except Exception as e:
#         logging.error(f"Gagal membaca file Excel: {e}")
#         sys.exit(1)

def read_excel(file_path, columns=None):
    try:
        if columns:
            df = pd.read_excel(file_path, usecols=columns)
        else:
            df = pd.read_excel(file_path)  # Baca semua kolom jika tidak ada filter
        logging.info(f"Kolom yang ada di file: {df.columns.tolist()}")  # Tambahkan log untuk melihat kolom yang tersedia
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


# def main():
#     engine = create_db_engine()
#     if not test_connection(engine):
#         logging.error("Akhiri skrip karena koneksi ke database gagal.")
#         sys.exit(1)
#     kolom_dipilih = choose_column()
#     column_mapping = get_column_mapping()
#
#     file_excel = 'D:/OneDrive/ASM/DATA/2023/04. APRIL 2023/20230405.xlsx'
#     nama_file = os.path.splitext(os.path.basename(file_excel))[0]
#
#     df = read_excel(file_excel, kolom_dipilih)
#     #logging.info("DataFrame setelah membaca Excel:")
#
#     df_filtered = df[df['Status'] != 'EXP'].copy()
#     df_filtered.insert(0, 'Periode', nama_file)
#     df_filtered.dropna(how='all', inplace=True)
#
#     # Pencocokan kolom dinamis dengan normalisasi nama kolom
#     dynamic_columns = get_dynamic_column(df_filtered.columns, column_mapping)
#     df_filtered.rename(columns=dynamic_columns, inplace=True)
#
#     # Sekarang update `numeric_columns` dan `date_columns` sesuai kolom yang telah dinormalisasi
#     numeric_columns = [dynamic_columns.get('pokokawal', 'PokokAwal'),
#                        dynamic_columns.get('saldopokok', 'SaldoPokok'),
#                        dynamic_columns.get('maxovd', 'MaxOvd'),
#                        dynamic_columns.get('angsuran', 'Angsuran')]
#     df_filtered = clean_numeric_columns(df_filtered, numeric_columns)
#
#     date_columns = [dynamic_columns.get('tgljt', 'TglJt'),
#                     dynamic_columns.get('tglcair', 'TglCair'),
#                     dynamic_columns.get('tgltarik', 'TglTarik'),
#                     dynamic_columns.get('tglbayar', 'TglBayar'),
#                     dynamic_columns.get('tgldo', 'TglDO'),
#                     dynamic_columns.get('tglproses', 'TglProses')]
#     df_filtered = convert_to_datetime(df_filtered, date_columns)
#
#     # Simpan data ke SQL
#     save_to_sql(df_filtered, engine, 'AZ_Daily')


# def main():
#     # Membuat koneksi ke database
#     engine = create_db_engine()
#
#     # Tes koneksi ke database
#     if not test_connection(engine):
#         logging.error("Akhiri skrip karena koneksi ke database gagal.")
#         sys.exit(1)
#
#     # Memilih kolom dari Excel dan memetakan ke kolom SQL
#     kolom_dipilih = choose_column()
#     column_mapping = get_column_mapping()
#
#     # Membaca file Excel
#     file_excel = 'D:/OneDrive/ASM/DATA/2023/04. APRIL 2023/20230405.xlsx'
#     nama_file = os.path.splitext(os.path.basename(file_excel))[0]
#
#     # Memanggil fungsi read_excel
#     df = read_excel(file_excel, kolom_dipilih)
#
#     # Filter data dan menambahkan kolom 'Periode'
#     df_filtered = df[df['Status'] != 'EXP'].copy()
#     df_filtered.insert(0, 'Periode', nama_file)
#     df_filtered.dropna(how='all', inplace=True)
#
#     # Pencocokan kolom dinamis dengan normalisasi nama kolom
#     dynamic_columns = get_dynamic_column(df_filtered.columns, column_mapping)
#     df_filtered.rename(columns=dynamic_columns, inplace=True)
#
#     # Menentukan kolom numerik dan tanggal berdasarkan kolom yang sudah dinormalisasi
#     numeric_columns = [dynamic_columns.get('pokokawal', 'PokokAwal'),
#                        dynamic_columns.get('saldopokok', 'SaldoPokok'),
#                        dynamic_columns.get('maxovd', 'MaxOvd'),
#                        dynamic_columns.get('angsuran', 'Angsuran')]
#     df_filtered = clean_numeric_columns(df_filtered, numeric_columns)
#
#     date_columns = [dynamic_columns.get('tgljt', 'TglJt'),
#                     dynamic_columns.get('tglcair', 'TglCair'),
#                     dynamic_columns.get('tgltarik', 'TglTarik'),
#                     dynamic_columns.get('tglbayar', 'TglBayar'),
#                     dynamic_columns.get('tgldo', 'TglDO'),
#                     dynamic_columns.get('tglproses', 'TglProses')]
#     df_filtered = convert_to_datetime(df_filtered, date_columns)
#
#     # Mengambil kolom dari tabel SQL dan menyimpan ke dalam database
#     kolom_tabel_sql = get_sql_table_columns(engine, 'AZ_Daily')
#     save_to_sql(df_filtered, engine, 'AZ_Daily')
#
#     logging.info("Selesai menyimpan data ke SQL.")


def main():
    engine = create_db_engine()

    # Tes koneksi ke database
    if not test_connection(engine):
        logging.error("Akhiri skrip karena koneksi ke database gagal.")
        sys.exit(1)

    # Pilih kolom dari source/column.py
    kolom_dipilih = choose_column()  # Normalisasi kolom yang dipilih
    column_mapping = get_column_mapping()

    file_excel = 'D:/OneDrive/ASM/DATA/2023/03. MARET 2023/20230312.xlsx'
    nama_file = os.path.splitext(os.path.basename(file_excel))[0]

    # Baca Excel tanpa filter kolom untuk melihat semua kolom yang ada
    df = read_excel(file_excel)

    # Logging kolom yang ada di file Excel
    logging.info(f"Kolom di file Excel: {df.columns.tolist()}")

    # Normalisasi nama kolom di file Excel
    df.columns = [normalize_column_name(col) for col in df.columns]
    logging.info(f"Kolom yang telah dinormalisasi: {df.columns.tolist()}")

    # Pencocokan kolom dinamis dengan normalisasi nama kolom
    dynamic_columns = get_dynamic_column(df.columns, column_mapping)
    logging.info(f"Kolom dinamis hasil pencocokan: {dynamic_columns}")

    # Filter hanya kolom yang diinginkan dan ubah nama kolom sesuai `column_mapping`
    df_filtered = df[list(dynamic_columns.keys())].copy()
    df_filtered = df_filtered[df_filtered['status'] != 'EXP'].copy()
    df_filtered.rename(columns=dynamic_columns, inplace=True)

    # Logging DataFrame setelah filter
    logging.info(f"DataFrame setelah filter dan rename kolom:\n{df_filtered.head()}")

    # Tambahkan kolom periode
    df_filtered.insert(0, 'Periode', nama_file)

    # Hapus baris yang kosong
    df_filtered.dropna(how='all', inplace=True)

    # Logging setelah menambahkan kolom periode dan penghapusan baris kosong
    logging.info(f"DataFrame setelah menambahkan kolom periode dan drop baris kosong:\n{df_filtered.head()}")

    # Sekarang update `numeric_columns` dan `date_columns` sesuai kolom yang telah dinormalisasi
    numeric_columns = [dynamic_columns.get('pokokawal', 'PokokAwal'),
                       dynamic_columns.get('saldopokok', 'SaldoPokok'),
                       dynamic_columns.get('maxovd', 'MaxOvd'),
                       dynamic_columns.get('angsuran', 'Angsuran')]

    df_filtered = clean_numeric_columns(df_filtered, numeric_columns)

    date_columns = [dynamic_columns.get('tgljt', 'TglJt'),
                    dynamic_columns.get('tglcair', 'TglCair'),
                    dynamic_columns.get('tgltarik', 'TglTarik'),
                    dynamic_columns.get('tglbayar', 'TglBayar'),
                    dynamic_columns.get('tgldo', 'TglDO'),
                    dynamic_columns.get('tglproses', 'TglProses')]

    df_filtered = convert_to_datetime(df_filtered, date_columns)

    # Logging setelah pembersihan kolom numerik dan konversi datetime
    logging.info(f"DataFrame setelah pembersihan kolom numerik dan konversi datetime:\n{df_filtered.head()}")

    # Simpan data ke SQL
    save_to_sql(df_filtered, engine, 'AZ_Daily')

    logging.info("Proses selesai, data berhasil disimpan ke database!")


if __name__ == "__main__":
    main()
