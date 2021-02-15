import pandas as pd
import os
from collections import defaultdict


def getId(folder):  # izveido sarakstu ar id numuriem no konrētas mapes
    with open('C://Users/tkcja/Desktop/Bakalaurs/DATI/' + folder + '/filenames.txt', 'r', encoding='utf-16') as f:
        idList = [line.strip() for line in f]
        return idList


def allData(expGroup,
            contGroup):  # izveido katrā(Izpētes un kontroles) mapē txt failu (_Dati_apstradei.txt) ar visiem datiem no failiem all_samples.kraken2.txt
    for group in expGroup, contGroup:
        savePath = 'C://Users/tkcja/Desktop/Bakalaurs/DATI/' + group + '/' + group + '_Dati_apstradei.txt'
        if os.path.exists(savePath):
            open(savePath, 'w').close()
        timePoint = ["T1", "T2"]
        for dati in getId(group):
            for time in timePoint:
                if not os.path.exists('C://Users/tkcja/Desktop/Bakalaurs/DATI/' + group + '/' + dati + '/' + time):
                    continue
                path = 'C://Users/tkcja/Desktop/Bakalaurs/DATI/' + group + '/' + dati + '/' + time + '/all_samples' \
                                                                                                     '.kraken2.txt '
                col_list = ["tot_all", "lvl_type", "name"]
                data_reader = pd.read_csv(path, skiprows=3, usecols=col_list, sep='\t')
                data_reader['ID'] = dati
                data_reader['Tests'] = time
                pd.set_option('display.expand_frame_repr', False, 'display.max_rows', 5000, 'display.max_columns', 10)
                data_reader = data_reader[["ID", "Tests", "tot_all", "lvl_type", "name"]]
                data_reader = data_reader[data_reader['lvl_type'] == "O"]
                data_reader.to_csv(savePath, mode='a', sep='\t', index=False)
    getBacteria(expGroup, contGroup)


def getBacteria(expGroup,
                contGroup):  # no liela txt faila _Dati_apstradei.txt tiek atlasītas bakterijas (kārtas/order) un tiek izveidots fails Apkopotas_Bakterijas(karta).txt katrā no mapēm
    for group in expGroup, contGroup:
        path = 'C://Users/tkcja/Desktop/Bakalaurs/DATI/' + group + '/' + group + '_Dati_apstradei.txt'
        savePath = 'C://Users/tkcja/Desktop/Bakalaurs/DATI/' + group + '/' + group + '_Apkopotas_Bakterijas(karta).txt'

        col_list = ["name"]
        data_reader = pd.read_csv(path, usecols=col_list, sep='\t', index_col=False)
        pd.set_option('display.max_rows', 5000)
        data_reader.name = data_reader.name.str.lstrip()
        data_reader.sort_values("name", inplace=True)
        data_reader.drop_duplicates(subset="name", inplace=True)
        data_reader = data_reader.head(-1)
        data_reader.to_csv(savePath, mode='w', index=False)

    createTable(expGroup, contGroup)


def createTable(expGroup,
                contGroup):  # katra bakterija no faila Apkopotas_Bakterijas iziet cauri failam  Dati_apstradei un pieraksta kuram pacientam un cik daudz bija ši bakterija. Tiek izveidots fails Visi_dati.csv
    for group in expGroup, contGroup:
        savePath = 'C://Users/tkcja/Desktop/Bakalaurs/DATI/' + group + '/' + group + '_Visi_dati.csv'
        alldata_reader = pd.read_csv(
            'C://Users/tkcja/Desktop/Bakalaurs/DATI/' + group + '/' + group + '_Dati_apstradei.txt', sep='\t')
        bacteria_reader = pd.read_csv(
            'C://Users/tkcja/Desktop/Bakalaurs/DATI/' + group + '/' + group + '_Apkopotas_Bakterijas(karta).txt',
            sep='\t')
        alldata_reader.name = alldata_reader.name.str.lstrip()

        newfile = pd.DataFrame({})
        newfile['ID'] = ''
        newfile['T'] = ''

        for bacteria in bacteria_reader["name"]:
            newfile[bacteria] = ''
        for col in newfile.columns:
            for alldata in alldata_reader.itertuples():

                if col == alldata.name:
                    newrow = {'ID': alldata.ID, 'T': alldata.Tests, col: alldata.tot_all}
                    newfile = newfile.append(newrow, ignore_index=True)

    newfile = alldata_reader.groupby(['ID', 'T'], as_index=False).sum()
    for column in newfile.columns[2:]:
        i = 0
        for items in newfile[column].iteritems():
            if items[1] >= 500:
                i = i + 1
        if newfile[[column]].mean()[0] >= 500 or (newfile[[column]].mean()[0] >= 100 and i >= 1):
            continue
        else:
            newfile = newfile.drop(column, axis=1)

    newfile.to_csv(savePath, mode='w', index=False)


def groupingData(expGroup,
                 contGroup):  # ši metode atgriež datus lasamā veidā un saglabājās gala faila _Apkopotie_dati.csv
    for group in expGroup, contGroup:
        alldata_reader = pd.read_csv('C://Users/tkcja/Desktop/Bakalaurs/DATI/' + group + '/' + group + '_Visi_dati.csv',
                                  sep=',')
        savepath = 'C://Users/tkcja/Desktop/Bakalaurs/DATI/' + group + '/' + group + '_Apkopotie_dati.csv'
        grouped = alldata_reader.groupby(['ID', 'T'], as_index=False).sum()
        for column in grouped.columns[2:]:
            i = 0
            for items in grouped[column].iteritems():
                if items[1] >= 1000:
                  i = i + 1
            if grouped[[column]].mean()[0] >= 1000 or (grouped[[column]].mean()[0] >= 500 and i >= 1):
                continue
            else:
                grouped = grouped.drop(column, axis=1)

        grouped.to_csv(savepath, mode='w', index=False)

allData('Izpetes_grupa', 'Kontroles_grupa')
