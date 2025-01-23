from pyspark.sql import SparkSession
import glob
import shutil
import time

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .appName('PySpark_data_processing') \
    .getOrCreate()
spark.sparkContext.setLogLevel("FATAL")
print(spark.sparkContext.appName)
all_csv_filenames = glob.glob('../Preliminary_preparation/received_files/*.csv')  # список всех csv_файлов
string_res = ""  # строка для формирования списка длительностей обработки каждого csv файла
with open('pyspark.txt', 'w', encoding='utf-8') as f:  # строка string_res записана в текстовый файл pyspark.txt
    f.write(string_res)
    f.close()
for csv_filename in all_csv_filenames:
    start = time.time()
    print(f" В обработке файл {csv_filename}: ")
    df = spark.read.csv(csv_filename, sep=',',
                        inferSchema=True, header=True)
    print(df.show(5))  # вывод на дисплей первых 5 строк сформированного дата фрейма
    num_rows = df.count()
    num_cols = len(df.columns)
    # вывод кол-строк и столбцов в датафрейме
    print(f'В файле строк - {num_rows} и столбцов - {num_cols}')
    # замена пропущенных значений на "Unknown" и вывод 10 строк
    print(f" Замена NaN на 'Unknown': ")
    df.na.fill('Unknown').show(10)
    #  вывод на печать типов данных для столбцов в датафрейме
    print(f'Типы данных для  столбцов в файле: ')
    dtypes = df.dtypes
    for column, dtype in dtypes:
        print(f'{column}: {dtype}')
    del dtypes
    #  вывод на дисплей статистики по всем столбцам фрейма данных
    summary = df.describe()
    print("Сводка статистики по всем столбцам фрейма данных :")
    summary.show(10)
    #  вывод на печать групп по столбцу "Пол"
    print(f'Группируем по столбцу "Пол":')
    df.groupBy('Sex').count().show()
    #  вывод на печать списка родившихся до 1950-01-01
    print(f'Список родившихся до 1950-01-01 :')
    df.filter(df.DateOfBirth < '1950-01-01').groupBy("DateOfBirth").count().sort("DateOfBirth").show(10)
    #  вывод на печать списка родившихся в интервале от 1950-01-01 до 2000-01-01
    print(f'Список родившихся в интервале от 1950-01-01 до 2000-01-01 :')
    (df.filter((df.DateOfBirth >= '1950-01-01') & (df.DateOfBirth < '2000-01-01')).groupBy("DateOfBirth").count()
     .sort("DateOfBirth").show(10))
    #  вывод на печать списка родившихся после 2000-01-01
    print(f'Список родившихся после 2000-01-01 :')
    df.filter(df.DateOfBirth >= '2000-01-01').groupBy("DateOfBirth").count().sort("DateOfBirth").show(10)
    print(print('*' * 50))
    end = time.time()
    res = end - start
    print(f'время = {res}')
    #  запись в текстовый файл данных о длительности обработки файлов
    string_res = string_res + ',' + str(res)
    with open('pyspark.txt', 'w', encoding='utf-8') as f:
        # f.write(str(list_time_work))
        f.write(string_res)
        f.close()
    del df
#  перемещение текстового файла данных о длительности обработки файлов в папку display_results
shutil.move('pyspark.txt', '../display_results/pyspark.txt')
spark.stop()
