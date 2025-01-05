import matplotlib.pyplot as plt


def form_list_numb(file):
    # функция формирует список из файла данных, который содержит затраченное время на
    # формирование дата фрейма из csv файла и обработку данных.
    f = open(file, 'r')
    f_r = f.read()
    # удаляет первый символ ','
    f_r_ = f_r[1:]
    # разделение строки на подстроки и перевод в вещественные числа
    list_numb = [float(x) for x in f_r_.split(",")]
    return list_numb


# Визуализация данных обработки csv файлов а приложениях на Pandas, Dask и Pyspark
y_p = form_list_numb('pandas.txt')
y_d = form_list_numb('dask.txt')
y_s = form_list_numb('pyspark.txt')

# х - размер csv файла в Гб
x = [0.11, 0.22, 0.44, 0.88, 1.76, 3.51, 7.03]

plt.plot(x, y_p, color='blue')
plt.plot(x, y_d, color='green')
plt.plot(x, y_s, color='red')

plt.xlabel('Размер csv файла в Гб')
plt.ylabel('Время в секундах')
plt.title('Время затраченное на обработку данных')
plt.annotate('Pandas', xy=(5.2, 2250), ha='right')
plt.annotate('Dask', xy=(6.5, 1500), ha='right')
plt.annotate('Pyspark', xy=(6.5, 750), ha='right')

plt.show()
