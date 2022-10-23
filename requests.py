import sqlite3
import pandas

database = sqlite3.connect("hotel.db.sqlite")
cursor = database.cursor()

print('\nТАБЛИЦА ГОСТИ')
dataFrame = pandas.read_sql('''SELECT * FROM Guests''', database)
print(dataFrame)

print('\nТАБЛИЦА ДОЛЖНОСТИ')
dataFrame = pandas.read_sql('''SELECT * FROM Post''', database)
print(dataFrame)

print('\nТАБЛИЦА СОТРУДНИКИ')
dataFrame = pandas.read_sql('''
                            SELECT
                                StaffID, FIO, Post, Salary
                            FROM HotelStaff
                                JOIN Post USING (PostID)
                            ''', database)
print(dataFrame)

print('\nТАБЛИЦА ТИПЫ СЕРВИСА')
dataFrame = pandas.read_sql('''SELECT * FROM TypeService''', database)
print(dataFrame)

print('\nТАБЛИЦА ТИПЫ КОМНАТ')
dataFrame = pandas.read_sql('''SELECT * FROM RoomType''', database)
print(dataFrame)

print('\nТАБЛИЦА КОМНАТЫ')
dataFrame = pandas.read_sql('''SELECT * FROM Rooms''', database)
print(dataFrame)

print('\nТАБЛИЦА РАЗМЕЩЕНИЕ')
dataFrame = pandas.read_sql('''SELECT * FROM Placement''', database)
print(dataFrame)

print('\nТАБЛИЦА ЕЖЕДНЕВНЫЙ УЧЕТ')
dataFrame = pandas.read_sql('''
                            SELECT 
                                RoomNum AS 'Номер комнаты',
                                StartServiceDate AS 'Начало обслуживания', 
                                FIO AS 'ФИО',
                                Post AS 'Должность',
                                TypeService AS 'Тип сервиса',
                                EndServiceDate AS 'Завершение обслуживания',
                                InspectionResult AS 'Результат осмотра(руб.)'
                            FROM DailyAccounting
                                JOIN HotelStaff USING (StaffID)
                                JOIN Post USING (PostID)
                                JOIN TypeService USING (ServiceID)
                            ''', database)
print(dataFrame)

print(f'\nI. ЗАПРОСЫ С УСЛОВИЯМИ И СОРТИРОВКОЙ:')
salary = 43000
print(f'1. ТАБЛИЦА СОТРУДНИКОВ С ЗАРПЛАТОЙ ВЫШЕ {salary} РУБЛЕЙ')
dataFrame = pandas.read_sql(f'''
                            SELECT
                                FIO, Salary
                            FROM HotelStaff
                                JOIN Post USING (PostID)
                            WHERE Salary > {salary}  
                            ORDER BY Salary
                            ''', database)
print(dataFrame)

print('\n2. ТАБЛИЦА ВЫВОДЯЩАЯ ВСЕ ПЛАНОВЫЕ ОСМОТРЫ ЗА ВСЕ ВРЕМЯ')
dataFrame = pandas.read_sql('''
                            SELECT 
                                FIO AS 'ФИО',
                                TypeService AS "Тип сервиса",
                                EndServiceDate AS "Завершение обслуживания"
                            FROM DailyAccounting
                                JOIN HotelStaff USING (StaffID)
                                JOIN TypeService USING (ServiceID)
                            WHERE TypeService = 'Плановый осмотр'
                            ORDER BY EndServiceDate DESC 
                            ''', database)
print(dataFrame)

print('\nII. ЗАПРОСЫ С ГРУППИРОВКОЙ')
print('1. ТАБЛИЦА ОТОБРАЖАЮЩАЯ КОЛИЧЕСТВО СПЕЦИАЛИСТОВ ОПРЕДЕЛЕННОЙ ДОЛЖНОСТИ')
dataFrame = pandas.read_sql('''
                            SELECT
                                Post,
                                COUNT(Post) AS "Количество специалистов"
                            FROM HotelStaff
                                JOIN Post USING (PostID)
                            GROUP BY Post
                            ''', database)
print(dataFrame)

print('\n2. ТАБЛИЦА ОТОБРАЖАЮЩАЯ КОЛИЧЕСТВО ГОСТЕЙ ОПРЕДЕЛЕННОГО ТИПА')
dataFrame = pandas.read_sql('''
                            SELECT 
                                TypeGuest,
                                COUNT(TypeGuest) AS "Количество гостей"
                            FROM Guests
                            GROUP BY TypeGuest
                            HAVING "Количество гостей" > 1
                            ''', database)
print(dataFrame)

post = "Горничная"
print('\nIII. ЗАПРОСЫ С ПОДЗАПРОСАМИ')
print(f'1. ТАБЛИЦА СОДЕРЖАЩАЯ ИНФОРМАЦИЯ О СОТРУДНИКАХ С ДОЛЖНОСТЬЮ "{post}"')
dataFrame = pandas.read_sql(f'''
                            SELECT
                                StaffID, 
                                FIO, 
                                (SELECT Post FROM Post WHERE PostID = HotelStaff.PostID and Post = "{post}") AS Должность
                            FROM HotelStaff
                            WHERE Должность not null 
                            ''', database)
print(dataFrame)

print('\n2. ТАБЛИЦА ВЫВОДЯЩАЯ РЕЗУЛЬТАТЫ ОСМОТРА, КОТОРЫЕ БОЛЬШЕ СРЕДНЕГО ЗНАЧЕНИЯ ЗА ВСЕ ВРЕМЯ')
dataFrame = pandas.read_sql('''
                            SELECT 
                                TypeService AS 'Тип сервиса',
                                EndServiceDate AS 'Завершение обслуживания',
                                InspectionResult AS 'Результат осмотра(руб.)'
                            FROM DailyAccounting
                                JOIN TypeService USING (ServiceID)
                            WHERE TypeService = "Плановый осмотр" 
                                and InspectionResult > (SELECT AVG(InspectionResult) FROM DailyAccounting)
                            ''', database)
print(dataFrame)

print('\nIV. ЗАПРОСЫ КОРРЕКТИРОВКИ ДАННЫХ')

staffID = 4034
FIO = "Иванов Иван Иванович"
postID = 300
print(f'1. ДОБАВЛЕНИЕ СОТРУДНИКА "{FIO}" В ТАБЛИЦУ "СОТРУДНИКИ"')
cursor.execute(f'''
                INSERT INTO HotelStaff (StaffID, FIO, PostID)
                VALUES ({staffID}, '{FIO}', {postID})
                ''')
database.commit()

dataFrame = pandas.read_sql('''SELECT StaffID, FIO FROM HotelStaff ORDER BY StaffID''', database)
print(dataFrame)

print(f'\n2. УДАЛЕНИЕ СОТРУДНИКА "{FIO}" ИЗ ТАБЛИЦЫ "СОТРУДНИКИ"')
cursor.execute(f'''DELETE FROM HotelStaff WHERE StaffID = {staffID}''')
database.commit()

dataFrame = pandas.read_sql('''SELECT StaffID, FIO FROM HotelStaff ORDER BY StaffID''', database)
print(dataFrame)

database.close()
