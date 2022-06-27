# Sanna Doolk
# sd222si
# 1dv503

import mysql.connector
from mysql.connector import errorcode
import csv

# Setup
cnx = mysql.connector.connect(user='root',
                             password='root',
                             #database= 'Sanna',
                             host='127.0.0.1:8889',
                             unix_socket='/Applications/MAMP/tmp/mysql/mysql.sock',)

DB_NAME = 'Sanna_PA2'

cursor = cnx.cursor()

# Method to create the database 
def create_database(cursor, DB_NAME):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
        print('Database created OK.')

    except mysql.connector.Error as err:
        print("Faild to create database {}".format(err))
        exit(1)

def create_candy_table(cursor):
    create_candy = "CREATE TABLE `candy` (" \
                     "  `name` varchar(255) NOT NULL," \
                     "  `type` varchar(255)," \
                     "  `taste` varchar(255)," \
                     "  `manufacturer` varchar(255)," \
                     "  PRIMARY KEY (`name`)" \
                     ")"

    try:
        print("Creating candy table: ")
        cursor.execute(create_candy)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table already exists.")
        else:
            print(err.msg)
    else:
        print("Created table OK")        

def create_candy_eater_table(cursor):
    create_candy_eater = "CREATE TABLE `candy_eater` (" \
                     "  `name` varchar(255) NOT NULL," \
                     "  `country` varchar(255)," \
                     "  `age` varchar(255)," \
                     "  `id` varchar(255)," \
                     "  `favourite_candy` varchar(255)," \
                     "  PRIMARY KEY (`id`)" \
                     ")"

    try:
        print("Creating candy_eater table: ")
        cursor.execute(create_candy_eater)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table already exists.")
        else:
            print(err.msg)
    else:
        print("Created table OK") 

def create_candy_manufacturer_table(cursor):
    create_candy_manifactures = "CREATE TABLE `candy_manufacturer` (" \
                     "  `name` varchar(255) NOT NULL," \
                     "  `funder` varchar(255)," \
                     "  `start_year` varchar(255)," \
                     "  PRIMARY KEY (`name`)" \
                     ")"

    try:
        print("Creating candy_manufacturer table: ")
        cursor.execute(create_candy_manifactures)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table already exists.")
        else:
            print(err.msg)
    else:
        print("Created table OK") 

def insert_values_candy_manufacturer(cursor):
  with open("data/candy_manufacturer.csv", "r") as file:
    csv_reader = csv.reader(file, skipinitialspace=False, delimiter=";")
    next(csv_reader)
    for row in csv_reader: #every row
            try:
                cursor.execute("""INSERT INTO candy_manufacturer (
                                name, funder, start_year)
                                VALUES(%s, %s, %s)""",
                                (row[0], row[1], row[2]))
            except mysql.connector.Error as err:
                print(err.msg)        
            else:
                cnx.commit() 

def insert_values_candy_eater(cursor):
  with open("data/candy_eaters.csv", "r") as file:
    csv_reader = csv.reader(file, skipinitialspace=False, delimiter=";")
    next(csv_reader)
    for row in csv_reader: #every row
            try:
                cursor.execute("""INSERT INTO candy_eater (
                                name, country, age, id, favourite_candy)
                                VALUES(%s, %s, %s, %s, %s)""",
                                (row[0], row[1], row[2], row[3], row[4]))
            except mysql.connector.Error as err:
                print(err.msg)        
            else:
                cnx.commit() 

def insert_values_candy(cursor):
  with open("data/Candy.csv", "r") as file:
    csv_reader = csv.reader(file, skipinitialspace=False, delimiter=";")
    next(csv_reader)
    for row in csv_reader: #every row
            try:
                cursor.execute("""INSERT INTO candy (
                                name, type, taste, manufacturer)
                                VALUES(%s, %s, %s, %s)""",
                                (row[0], row[1], row[2], row[3]))
            except mysql.connector.Error as err:
                print(err.msg)        
            else:
                cnx.commit() 

def create_database_and_tables():
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exist".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor, DB_NAME)
            print("Database {} created succesfully.".format(DB_NAME))
            cnx.database = DB_NAME
            create_candy_table(cursor)
            insert_values_candy(cursor)
            create_candy_eater_table(cursor)
            insert_values_candy_eater(cursor)
            create_candy_manufacturer_table(cursor)
            insert_values_candy_manufacturer(cursor)

        else:
            print(err) 

# Shows number of candy eaters in database per country 
# Aggregation, grouping - 1 table
def countries_of_candy_eaters(cursor):
    query = 'SELECT COUNT(id), country FROM candy_eater GROUP BY country'
    cursor.execute(query)
    result = cursor.fetchall()
    for x in result:
        print(x)

#JOIN
def show_name_favourite_candy_and_taste(cursor):
    query = 'SELECT candy_eater.name, candy_eater.favourite_candy, candy.taste FROM candy_eater INNER JOIN candy ON candy_eater.favourite_candy=candy.name'
    cursor.execute(query)
    result = cursor.fetchall()
    for x in result:
        print(x)

#JOIN
def show_candy_manufacturer_and_funder(cursor):
    candy = input('Enter the name of a candy: ') 
    candy = (candy, )
    query = 'SELECT candy.name, candy.manufacturer, candy_manufacturer.funder FROM candy INNER JOIN candy_manufacturer ON candy.manufacturer=candy_manufacturer.name WHERE candy.name=%s'
    cursor.execute(query, candy)
    result = cursor.fetchall()
    for x in result:
        print(x)

#JOIN
def taste_of_favourite_candy_average_age(cursor): 
    taste = input('Enter a taste: ')  
    taste = (taste, )
    query = 'SELECT AVG(candy_eater.age) FROM candy_eater INNER JOIN candy ON candy_eater.favourite_candy=candy.name WHERE candy.taste=%s'
    cursor.execute(query, taste)
    result = cursor.fetchall()
    for x in result:
        print(x)

#VIEW
def view_name_and_favourite_candy_by_country(cursor):
    country = input('Enter a country: ') 
    country = (country, )       
    view = 'CREATE OR REPLACE VIEW name_and_favourite_candy_by_country AS SELECT name, favourite_candy FROM candy_eater WHERE country = %s'
    cursor.execute(view, country)
    query = 'SELECT * FROM name_and_favourite_candy_by_country'
    cursor.execute(query)
    result = cursor.fetchall()
    for x in result:
        print(x)

def show_manufacturers_based_on_start_year(cursor):
    year = input('Enter a year: ') 
    year = (year, )
    query = 'SELECT name FROM candy_manufacturer WHERE start_year<%s'
    cursor.execute(query, year)
    result = cursor.fetchall()
    for x in result:
        print(x)   

# Shows the menu 
def print_menu_options():
    options = {
        1: '1. Show number of candy eaters in database per country',
        2: '2. Show name of candy eaters, their favourite candy and the taste of that candy',
        3: '3. Show the manufacturer and its funder of a specific candy',
        4: '4. Show the average age of candy eaters who have a favourite candy of a specific taste',
        5: '5. Show the name and favourite candy for candy eaters in a specific country',
        6: '6. Show manufacturers that were founded before a specific year',
        7: '7. Quit',
    }
    print('-----------------------------')
    for key in options.keys():
        print (options[key] )
    print('-----------------------------')    

def show_menu(cursor):
    show = True
    while(show):
        print_menu_options() 
        try:
            choice = int(input('Enter your choice: '))
        except:
            print('Please enter a number between 1 and 6.')

        if choice == 1:
            countries_of_candy_eaters(cursor)
            input('Press ENTER to go back to menu') 
        elif choice == 2:
            show_name_favourite_candy_and_taste(cursor)
            input('Press ENTER to go back to menu')
        elif choice == 3:
            show_candy_manufacturer_and_funder(cursor)
            input('Press ENTER to go back to menu')
        elif choice == 4:
            taste_of_favourite_candy_average_age(cursor)
            input('Press ENTER to go back to menu')
        elif choice == 5:
            view_name_and_favourite_candy_by_country(cursor)
            input('Press ENTER to go back to menu')
        elif choice == 6:
            show_manufacturers_based_on_start_year(cursor)
            input('Press ENTER to go back to menu')
        elif choice == 7:
            show = False
            print('Closing.')    
        else:
            print('Not an option. Please enter a number between 1 to 6.')

create_database_and_tables()

show_menu(cursor)
