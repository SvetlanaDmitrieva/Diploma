import dask.dataframe as dd
import glob
import shutil
import time


all_csv_filenames = glob.glob('../Preliminary_preparation/received_files/*.csv')  # список всех csv_файлов
string_res = ""         # строка для формирования списка длительностей обработки каждого csv файла
with open('dask.txt', 'w', encoding='utf-8') as f:       # строка string_res записана в текстовый файл dask.txt
    f.write(string_res)
    f.close()
for csv_filename in all_csv_filenames:
    start = time.time()
    print(f" В обработке файл {csv_filename}: ")
    df = dd.read_csv(csv_filename)
    print(df.head())        # вывод на дисплей первых 5 строк сформированного дата фрейма
    # вывод кол-строк и столбцов в дата фрейме
    print(f'В файле строк - {df.shape[0].compute(num_workers=8)} и столбцов - {df.shape[1]}')
    # замена пропущенных значений на "Unknown"
    print(f" Замена NaN на 'Unknown': ")
    print(df.fillna('Unknown').compute(num_workers=8))
    #  вывод на дисплей статистики по всем столбцам фрейма данных
    print('Сводка статистики по всем столбцам фрейма данных : ')
    print(df.describe(include='all').compute(num_workers=8))
    #  вывод на печать типов данных для столбцов в дата фрейме
    print(f'Типы данных для  столбцов в файле: ')
    print(df.dtypes)
    #  вывод на печать групп по столбцу "Пол"
    print(f'Группируем по столбцу " ол":')
    print(df.groupby('Sex').size().compute(num_workers=8))
    #  вывод на печать списка родившихся до 1950-01-01
    print(f'Список родившихся до 1950-01-01 : ')
    print(df.loc[(df['DateOfBirth'] < '1950-01-01'), 'DateOfBirth'].compute(num_workers=8))
    #  вывод на печать списка родившихся в интервале от 1950-01-01 до 2000-01-01
    print(f'Список родившихся в интервале от 1950-01-01 до 2000-01-01 :')
    print(df.loc[(df['DateOfBirth'] >= '1950-01-01') & (df['DateOfBirth'] < '2000-01-01'), 'DateOfBirth']
            .compute(num_workers=8))
    #  вывод на печать списка родившихся после 2000-01-01
    print(f'Список родившихся после 2000-01-01 :')
    print(df.loc[(df['DateOfBirth'] >= '2000-01-01'), 'DateOfBirth'].compute(num_workers=8))
    print(print('*' * 50))
    end = time.time()
    res = end - start
    print(f'время = {res}')
    #  запись в текстовый файл данных о длительности обработки файлов
    string_res = string_res + ',' + str(res)
    with open('dask.txt', 'w', encoding='utf-8') as f:
        f.write(string_res)
        f.close()
#  перемещение текстового файла данных о длительности обработки файлов в папку display_results
shutil.move('dask.txt', '../display_results/dask.txt')
