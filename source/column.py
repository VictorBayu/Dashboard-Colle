from imports import *


def normalize_column_name(col_name):
    """Normalize column names by making them lowercase and removing spaces or underscores."""
    return re.sub(r'\s|_', '', col_name.strip().lower())


def get_column_mapping():
    column_mapping = {
        'BranchID': 'BranchID',
        'AgreementNo': 'AgreementNo',
        'CustomerID': 'CustomerID',
        'Nama': 'Nama',
        'Model': 'Model',
        'Tenor': 'Tenor',
        'Ang_ke': 'Ang_ke',
        'PokokAwal': 'Pokok_Awal',
        'SaldoPokok': 'Saldo_Pokok',
        'Ovd': 'OVD',
        'Status': 'Status',
        'TglJt': 'Tgl_Jt',
        'TglCair': 'Tgl_Cair',
        'ProductId': 'ProductId',
        'StatusWo': 'StatusWo',
        'StatusAsset': 'StatusAsset',
        'TglTarik': 'Tgl_Tarik',
        'TglBayar': 'Tgl_Bayar',
        'TglDO': 'TglDO',
        'TglProses': 'Tgl_Proses',
        'MaxOvd': 'Max_OVD',
        'POS_ID': 'POS_ID',
        'ApplicationPriority': 'ApplicationPriority',
        'BAname': 'BAName',
        'CMO': 'CMO',
        'Angsuran': 'Angsuran',
        'ProfID': 'Profession',
        'SourceApplication': 'SourceApplication'
    }
    return {normalize_column_name(k): v for k, v in column_mapping.items()}


def choose_column():
    choosen = [
        'BranchID', 'AgreementNo', 'CustomerID', 'Nama', 'Model', 'Tenor', 'AngKe',
        'PokokAwal', 'SaldoPokok', 'Ovd', 'Status', 'TglJt', 'TglCair', 'ProductId', 'StatusWo',
        'StatusAsset', 'TglTarik', 'TglBayar', 'TglDO', 'TglProses', 'MaxOvd', 'POS_ID', 'ApplicationPriority',
        'BAname', 'CMO', 'Angsuran', 'ProfID', 'SourceApplication'
    ]
    return [normalize_column_name(col) for col in choosen]
