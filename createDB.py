import sqlite3

database = sqlite3.connect("hotel.db.sqlite")

database.executescript('''
                    DROP TABLE IF EXISTS RoomType;
                    CREATE TABLE RoomType (
                      TypeID INT UNSIGNED NOT NULL,
                      RoomType TEXT(40) NOT NULL,
                      Price INT NOT NULL,
                      PRIMARY KEY (TypeID)
                    );
                    
                    DROP TABLE IF EXISTS Rooms;
                    CREATE TABLE Rooms (
                      RoomNum INT UNSIGNED NOT NULL,
                      Places INT UNSIGNED NOT NULL,
                      RoomFeatures MEDIUMTEXT NULL,
                      Floor INT UNSIGNED NOT NULL,
                      TypeID INT UNSIGNED NOT NULL,
                      RoomStatus VARCHAR(45) NOT NULL,
                      PRIMARY KEY (RoomNum),
                        FOREIGN KEY (TypeID)
                        REFERENCES RoomType (TypeID)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE
                    );
                    
                    DROP TABLE IF EXISTS Guests;
                    CREATE TABLE Guests (
                      PassportNum INT UNSIGNED NOT NULL,
                      FIO VARCHAR(150) NOT NULL,
                      Citizenship VARCHAR(45) NOT NULL,
                      TypeGuest VARCHAR(45) NOT NULL,
                      Discount INT NULL,
                      PRIMARY KEY (PassportNum)
                    );
                    
                    DROP TABLE IF EXISTS Placement;
                    CREATE TABLE Placement (
                      SetDate DATETIME NOT NULL,
                      DepartureDate DATETIME NULL,
                      RoomNum INT UNSIGNED NOT NULL,
                      PassportNum INT UNSIGNED NOT NULL,
                      PRIMARY KEY (RoomNum, PassportNum),
                        FOREIGN KEY (RoomNum)
                        REFERENCES Rooms (RoomNum)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE,
                        FOREIGN KEY (PassportNum)
                        REFERENCES Guests (PassportNum)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE
                    );
                    
                    DROP TABLE IF EXISTS Post;
                    CREATE TABLE Post (
                      PostID INT NOT NULL,
                      Post VARCHAR(45) NOT NULL,
                      Salary INT NOT NULL,
                      PRIMARY KEY (PostID)
                    );
                    
                    DROP TABLE IF EXISTS HotelStaff;
                    CREATE TABLE HotelStaff (
                      StaffID INT UNSIGNED NOT NULL,
                      FIO VARCHAR(150) NOT NULL,
                      PostID INT NOT NULL,
                      PRIMARY KEY (StaffID, PostID),
                        FOREIGN KEY (PostID)
                        REFERENCES Post (PostID)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE
                    );
                    
                    
                    DROP TABLE IF EXISTS TypeService;
                    CREATE TABLE TypeService (
                      ServiceID INTEGER PRIMARY KEY AUTOINCREMENT,
                      TypeService VARCHAR(45) NOT NULL
                    );
                    
                    
                    DROP TABLE IF EXISTS DailyAccounting;
                    CREATE TABLE DailyAccounting (
                      StartServiceDate DATETIME NOT NULL,
                      EndServiceDate DATETIME,
                      InspectionResult INT,
                      RoomNum INT UNSIGNED NOT NULL,
                      PassportNum INT UNSIGNED NOT NULL,
                      StaffID INT UNSIGNED NOT NULL,
                      ServiceID INT NOT NULL,
                      
                      PRIMARY KEY (RoomNum, PassportNum, StaffID, ServiceID),
                      
                        FOREIGN KEY (RoomNum , PassportNum)
                        REFERENCES Placement (RoomNum , PassportNum)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE,
                        
                        FOREIGN KEY (StaffID)
                        REFERENCES HotelStaff (StaffID)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE,
                        
                        FOREIGN KEY (ServiceID)
                        REFERENCES TypeService (ServiceID)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE
                    );
                    
                    
                    INSERT INTO Guests (PassportNum, FIO, Citizenship, TypeGuest, Discount)
                    values
                    (866743, 'Максимова Агата Никитична', 'Россия', 'Обычный', null),
                    (491364, 'Семенов Макар Дмитриевич', 'Норвегия', 'Обычный', null),
                    (310846, 'Черкасов Захар Матвеевич', 'Индий', 'Постоянный', 800),
                    (738952, 'Лукьянова Стефания Дмитриевна', 'Греция', 'VIP', null),
                    (169003, 'Иванова Ева Львовна', 'Македония', 'VIP', 1300);
                    
                    INSERT INTO Post (PostID, Post, Salary)
                    values
                    (100, 'Горничная', 32000),
                    (200, 'Сантехник', 45000),
                    (300, 'Электрик', 43000),
                    (400, 'Администратор', 55000);
                    
                    INSERT INTO HotelStaff (StaffID, FIO, PostID)
                    values
                    (1346, 'Фролова Милана Максимовна', 100),
                    (3401, 'Соболев Илья Максимович', 300),
                    (2678, 'Баранова Ева Ивановна', 100),
                    (7118, 'Коновалов Иван Миронович', 200),
                    (3256, 'Козина Анна Романовна', 100),
                    (8693, 'Щербаков Тимур Иванович', 200),
                    (5947, 'Родин Илья Александрович', 300),
                    (6891, 'Мальцев Михаил Артёмович', 300),
                    (9600, 'Артемов Александр Игоревич', 200),
                    (1212, 'Иванов Тимофей Максимович', 400),
                    (1313, 'Алексеева Василиса Максимовна', 400);
                    
                    INSERT INTO TypeService (ServiceID, TypeService)
                    values
                    (10, 'Уборка'),
                    (20, 'Смена белья'),
                    (30, 'Ремонт сантехники'),
                    (40, 'Ремонт электроники'),
                    (50, 'Плановый осмотр');
                    
                    INSERT INTO RoomType (TypeID, RoomType, Price)
                    values
                    (1100, 'Однокомнатный', 4400),
                    (2200, 'Двухкомнатный', 5700),
                    (5601, 'Люкс', 6600),
                    (6301, 'Апартамент', 7400),
                    (2802, 'Семейный', 9800),
                    (3000, 'Сюит', 11200);
                    
                    INSERT INTO Rooms (RoomNum, Places, RoomFeatures, Floor, TypeID, RoomStatus)
                    values
                    (145, 2, null, 1, 1100, 'свободно'),
                    (201, 3, null, 2, 2200, 'свободно'),
                    (426, 2, 'Кабинет, телевизор', 4, 5601, 'занято'),
                    (351, 4, 'Телефизор, зона для игр', 3, 2802, 'обслуживается'),
                    (457, 3, 'Кухня, кабинет, телевизор', 4, 6301, 'занято'),
                    (505, 6, 'Гостинная, кабинет, телевизор, проводной интернет', 5, 3000, 'свободно'),
                    (137, 4, null, 1, 2200, 'обслуживается'),
                    (309, 2, 'Кабинет, телевизор', 4, 5601, 'свободно'),
                    (346, 4, 'Телефизор, зона для игр', 3, 2802, 'свободно'),
                    (161, 2, null, 1, 1100, 'обслуживается');
                    
                    INSERT INTO Placement (RoomNum, PassportNum, SetDate, DepartureDate)
                    values
                    (201, 866743, '2022-01-27 16:23:56', null),
                    (145, 491364, '2022-03-13 10:01:34', '2022-03-14 09:14:08'),
                    (505, 169003, '2022-02-08 13:22:11', null),
                    (346, 738952, '2022-03-17 17:11:29', '2022-03-20 17:00:21'),
                    (309, 310846, '2022-01-04 06:45:57', null);
                    
                    INSERT INTO DailyAccounting (StartServiceDate, EndServiceDate, InspectionResult, RoomNum, PassportNum, StaffID, ServiceID)
                    values
                    ('2022-03-14 09:14:08', '2022-03-14 09:57:41', 1700, 145, 491364, 1212, 50),
                    ('2022-03-14 09:14:08', '2022-03-14 12:03:56', null, 145, 491364, 1346, 20),
                    ('2022-03-20 17:00:21', '2022-03-20 19:46:02', 4240, 346, 738952, 1313, 50),
                    ('2022-03-20 17:00:21', '2022-03-20 18:34:12', null, 346, 738952, 2678, 20);
                        ''')

database.commit()
database.close()
