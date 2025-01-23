import pandas as pd
import os
import glob
import sys


def preparation_file(source_file, size_frame, name_new_file):
    # функция удваивает объем дата фрейма до размера size_frame, после записывает
    # полученный дата фрейм в csv файл name_new_file
    df1 = pd.read_csv(source_file)
    work_df = sys.getsizeof(df1) / 1048576
    while work_df < size_frame / 2 + 0.1:
        df1 = df1._append(df1)
        work_df = sys.getsizeof(df1) / 1048576
    df1.to_csv(name_new_file, float_format="%.2f", index=False)
    del df1


# Объединение всех csv-файлов в серию файлов
csv_filename = glob.glob('../source_file/*.csv')  # считывание csv_файла
print(f'csv_filename = {csv_filename}')
print('Для анализа получены:')
preparation_file(csv_filename[0], 800, 'csv_00.csv')
print(f' создан новый файл csv_00.csv, размер {(os.path.getsize("csv_00.csv") / 1048576):.2f}')
df = pd.read_csv('csv_00.csv', index_col=0)
size_df = int(sys.getsizeof(df)) / 1048576
del df
name_old_file = "csv_00.csv"
print('*' * 50)
# последовательно формируется 6 csv файлов путем удваивания объема предыдущего
for i in range(1, 7):
    if i > 9:
        file_name = 'csv_' + str(i) + '.csv'
    else:
        file_name = 'csv_' + '0' + str(i) + '.csv'
    preparation_file(name_old_file, size_df * 2 ** i, file_name)
    print(f' создан новый файл {file_name}, размер {(os.path.getsize(file_name) / 1048576):.2f}')
    print('*' * 50)
    name_old_file = file_name
