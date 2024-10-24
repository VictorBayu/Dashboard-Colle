from imports import *
from insert import (read_excel, clean_numeric_columns, convert_to_datetime, create_db_engine,
                    test_connection, get_sql_table_columns, save_to_sql, get_dynamic_column)
from source.column import *

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

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.xlsx'):
            file_path = os.path.join(folder_path, file_name)
            nama_file = os.path.splitext(file_name)[0]
            df = read_excel(file_path, choose_column())
            logging.info(f"DataFrame setelah membaca {file_name}:")

            df_filtered = df[df['Status'] != 'EXP'].copy()
            df_filtered.insert(0, 'Periode', nama_file)
            df_filtered.dropna(how='all', inplace=True)

            dynamic_columns = get_dynamic_column(df_filtered.columns, get_column_mapping())
            df_filtered.rename(columns=dynamic_columns, inplace=True)

            numeric_columns = [dynamic_columns.get('PokokAwal', 'PokokAwal'),
                               dynamic_columns.get('SaldoPokok', 'SaldoPokok'),
                               dynamic_columns.get('MaxOvd', 'MaxOvd'),
                               dynamic_columns.get('Angsuran', 'Angsuran')]
            df_filtered = clean_numeric_columns(df_filtered, numeric_columns)

            date_columns = [dynamic_columns.get('TglJt', 'TglJt'),
                            dynamic_columns.get('TglCair', 'TglCair'),
                            dynamic_columns.get('TglTarik', 'TglTarik'),
                            dynamic_columns.get('TglBayar', 'TglBayar'),
                            dynamic_columns.get('TglDO', 'TglDO'),
                            dynamic_columns.get('TglProses', 'TglProses')]
            df_filtered = convert_to_datetime(df_filtered, date_columns)

            save_to_sql(df_filtered, engine, 'AZ_Daily')


def main():
    folder_path = 'D:/OneDrive/ASM/DATA/2023/04. APRIL 2023'
    process_files_in_folder(folder_path)


if __name__ == "__main__":
    main()
