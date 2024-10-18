import pandas as pd

# Path ke file Excel
file_excel = 'D:/OneDrive/ASM/DATA/2023/01. JANUARI 2023/20230101.xlsx'

# Membaca sheet pertama untuk mendapatkan nama kolom
df_sample = pd.read_excel(file_excel, nrows=0)

# Menampilkan nama kolom yang ada
print(df_sample.columns.tolist())
