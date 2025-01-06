import pandas as pd
import glob
import shutil
import time


all_csv_filenames = glob.glob('../Preliminary_preparation/received_files/*.csv')  # список всех csv_файлов
string_res = ""         # строка для формирования списка длительностей обработки каждого csv файла
with open('pandas.txt', 'w', encoding='utf-8') as f:         # строка string_res записана в текстовый файл pandas.txt
    f.write(string_res)
    f.close()
for csv_filename in all_csv_filenames:
    j = rows = columns = 0
    # подготовка списка results выводимых статистических данных
    results = []
    day_of_birt1 = day_of_birt2 = day_of_birt3 = dict_group = pd.DataFrame()
    start = time.time()
    print(f" В обработке файл {csv_filename}: ")
    # чтение файла по 10 000 000 строк, учитывая, что существует ограничение на объем свободного ОЗУ
    for i in pd.read_csv(csv_filename, sep=',', chunksize=10000000):
        if j == 0:
            columns = i.shape[1]
            # вывод на дисплей первых 5 строк сформированного дата фрейма
            print(f'{i.head()}')
            # замена пропущенных значений на "Unknown"
            print(f" Замена NaN на : 'Unknown'")
            i.fillna('Unknown', inplace=True)
            print(i)
            #  вывод на печать типов данных для столбцов в файле
            print(f'Типы данных для  столбцов в файле: ')
            print(i.dtypes)
            #  формирование групп по столбцу "Пол"
            dict_group = i.groupby('Sex').size()
            #  формирование списка родившихся до 1950-01-01
            day_of_birt1 = i.loc[i['DateOfBirth'] < '1950-01-01']
            #  формирование списка родившихся в интервале от 1950-01-01 до 2000-01-01
            day_of_birt2 = i.loc[(i['DateOfBirth'] >= '1950-01-01') & (i['DateOfBirth'] < '2000-01-01')]
            #  формирование списка родившихся после 2000-01-01
            day_of_birt3 = i.loc[i['DateOfBirth'] >= '2000-01-01']
        else:
            i.fillna('Unknown', inplace=True)
            dict_sex = i.groupby('Sex').size()
            for k, v in dict_group.items():
                if dict_sex[k].any():
                    dict_group[k] = v + dict_sex[k]
            day_of_birt1_new = i.loc[i['DateOfBirth'] < '1950-01-01']
            day_of_birt2_new = i.loc[(i['DateOfBirth'] >= '1950-01-01') & (i['DateOfBirth'] < '2000-01-01')]
            day_of_birt3_new = i.loc[i['DateOfBirth'] >= '2000-01-01']
            if not day_of_birt1_new.empty:
                day_of_birt_app = pd.concat([day_of_birt1, day_of_birt1_new])
                day_of_birt1 = day_of_birt_app
            if not day_of_birt2_new.empty:
                day_of_birt_app = pd.concat([day_of_birt2, day_of_birt2_new])
                day_of_birt2 = day_of_birt_app
            if not day_of_birt3_new.empty:
                day_of_birt_app = pd.concat([day_of_birt3, day_of_birt3_new])
                day_of_birt3 = day_of_birt_app
        j += 1
        rows += i.shape[0]
        describe_chunk = i.describe(include='all')
        results.append(describe_chunk)
    results0 = pd.concat(results)
    results1 = results0.describe()
    print(f'В файле строк - {rows} и столбцов - {columns}')
    print('Сводка статистики по всем столбцам фрейма данных : ')
    print(results1)
    #  вывод на печать групп по столбцу "Пол"
    print(f'Группировка по столбцу "Пол":')
    for k, v in dict_group.items():
        print(f'{k} : {v}')
    #  вывод на печать списка родившихся до 1950-01-01
    print(f'Список родившихся до 1950-01-01 :')
    print(day_of_birt1)
    #  вывод на печать списка родившихся в интервале от 1950-01-01 до 2000-01-01
    print(f'Список родившихся в интервале от 1950-01-01 до 2000-01-01 :')
    print(day_of_birt2)
    #  вывод на печать списка родившихся после 2000-01-01
    print(f'Список родившихся после 2000-01-01 :')
    print(day_of_birt3)
    print(print('*' * 50))
    end = time.time()
    res = end - start
    print(f'время = {res}')
    #  запись в текстовый файл данных о длительности обработки файлов
    string_res = string_res + ',' + str(res)
    del i
    with open('pandas.txt', 'w', encoding='utf-8') as f:
        f.write(string_res)
        f.close()
#  перемещение текстового файла данных о длительности обработки файлов в папку display_results
shutil.move('pandas.txt', '../display_results/pandas.txt')
