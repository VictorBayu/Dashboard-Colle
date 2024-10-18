import pyodbc

connection_string = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=Reporting;'
    'Trusted_Connection=yes;'
)

try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    print("Hasil query:", result[0])
    conn.close()
except Exception as e:
    print(f"Gagal terhubung ke database: {e}")


mapping_df_to_sql = {
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
        'MaxOvd': 'Max OVD',
        'POS_ID': 'POS_ID',
        'ApplicationPriority': 'ApplicationPriority',
        'BAname': 'BAName',
        'CMO': 'CMO',
        'Angsuran': 'Angsuran',
        'ProfID': 'Profession',
        'SourceApplication': 'SourceApplication'
    }