# -*- coding: utf-8 -*-
"""
Created on Wed May  6 18:02:13 2020

@author: Antonio Rivera Lopez
Database project for COMP4018
"""

import mysql.connector
import pandas as pd
import numpy as np
import math


#Connect to the server

h = input("Please enter your host name\n").strip()
u = input("Please enter your user\n").strip()
pas = input("Please enter your password\n").strip()
d = input("Please enter the database you will be using\n").strip()
mydb = mysql.connector.connect(host=h, user=u,passwd=pas, database=d) #Connect to DB
print(mydb)
print("\nConnected successfully\n")




    
###########################   C.R.U.D Functions   ###############################

def addSample(filename): #Function that adds samples from csv files and creates a table for the data
    
 
    myCursor = mydb.cursor() #The cursor that gives python access to the database 
    
    tableName = input("Enter the name of the table you want to create\n")
    
    myCursor.execute("""CREATE TABLE {} (SNP_Name VARCHAR(80) PRIMARY KEY, GC_Score DECIMAL(6,5), 
                     GT_Score DECIMAL(6,5), Position INT, Chr VARCHAR(5), 
                     Allele1Top VARCHAR(1), Allele2Top VARCHAR(1), SNP VARCHAR(5), SampleID VARCHAR(80))""".format(tableName))
    
    
    #Create table IDSNP for each sample
    
    

    
    # Create a dataframe from csv
    df = pd.read_csv(filename, delimiter=',')
    
    
    dfSNP = df.loc[:, ['SNP Name', 'GC Score', 'GT Score', 'Position', 'Chr', 'Allele1 - Top', 
                       'Allele2 - Top', 'SNP', 'Sample ID']]


        
    
################################################### Change ',' for '.'
    
    #Replace missing values with mean
    X = dfSNP.to_numpy()
    
    
    for i in range(0,len(X)):
  
#######Replace empty values with zero#######################
        if( type(X[i][1]) is not str and math.isnan(X[i][1])):
            X[i][1] = np.nan_to_num(X[i][1])
            
        if( type(X[i][2]) is not str and math.isnan(X[i][2])):
            X[i][2] = np.nan_to_num(X[i][2])
    #############################################################
            
        
        
        if(type(X[i][1]) is str):
            a = X[i][1].replace(',', '.',1)
            
        if(type(X[i][2]) is str):  
            b = X[i][2].replace(',', '.',1)
    
        X[i][1] = float(a)
        X[i][2] = float(b)
######################################################
    
    # Create a list of tuples 
    tupleSNP= [tuple(row) for row in X] 

    snpFormula = """INSERT INTO {} (SNP_Name, GC_Score, GT_Score, Position, 
    Chr, Allele1Top, Allele2Top, SNP, SampleID) VALUES 
    (%s, %s, %s, %s, %s, %s, %s, %s, %s)""".format(tableName)
    
    
    n = 0
    
    for i in tupleSNP:
         myCursor.execute(snpFormula, i)
         n+=1
         if(n==len(X)):
             print('\nDone\n\n')
             break
        
    mydb.commit()
#End of function    
    
#Remember that addSample is a very delicate function, with one RUN you create a table permanently

#addSample('PR_random control.csv')


def printData(tableName): #Function that lets you print data from the table
    myCursor = mydb.cursor()
    
    print("Press 1 to print all data in table. ONLY IF ROWS ARE LESS THAN 1000")
    print("Press 2 to print data by attribute \n")
    choice = input("Choose your print option: \n")
    
    if(int(choice) == 1):
        try:
            myCursor.execute("SELECT * FROM {}".format(tableName))
            tableResult = myCursor.fetchall()
        
            for row in tableResult:
                print('\n',row)
        
        except:
            print("\n\nNo table named {}".format(tableName))
        
            
            
    elif(int(choice) == 2):
        atr = input("Write the attribute you want printed \n")
        myCursor.execute("SELECT {a} FROM {b}".format(a = atr, b=tableName))
        columnResult = myCursor.fetchall()
        
        for r in columnResult:
            print('\n',r)
    
    else:
        print("That is not an option")
    
 
def insert(tableName, t): #Function that inserts a row of data to the table
    myCursor = mydb.cursor()
    insertFormula = """INSERT INTO {} (SNP_Name, GC_Score, GT_Score, Position, 
    Chr, Allele1Top, Allele2Top, SNP, SampleID) VALUES 
    (%s, %s, %s, %s, %s, %s, %s, %s, %s)""".format(tableName)
    
    
    myCursor.execute(insertFormula, t)
    mydb.commit()
    

def getInput(): #function that recieves and preprocesses user input
    x1 = input("Enter SNP name\n")
    x2 = float(input("Enter GC Score\n"))
    #Check if inside correct range
    if(x2 > 1):
         print("Out of range value")
         return 0
     
    elif(x2 < 0):
        print("Out of range value")
        return 0
    
    x3 = float(input("Enter GT Score\n"))
    #Check if inside correct range
    if(x3 > 1):
         print("Out of range value")
         return 0
     
    elif(x3 < 0):
        print("Out of range value")
        return 0
    
    x4 = int(input("Enter Position\n"))
    if(x4>999999999):
        print("Out of range position value\n")
        return 0
    
    
    x5 = input("Enter Chromosome\n")
    #Check if Chromosomes are in the correct range [1...22, X, Y]    
    if(x5=='X' or x5=='Y'):
        True
    elif(int(x5) < 1 or int(x5) > 22):
            print("Chromosome value out of range")
            return 0
        
    x6 = input("Enter Allele1\n")
    #Check if alleles are correct
    if(x6 != 'A' and x6 != 'T' and x6 != 'C' and x6 != 'G' and x6 != 'D' and x6 != 'I'):
        print("\nIncorrect allele1. Choose from ATCGDI")
        return 0
    
    x7 = input("Enter Allele2\n")
    #Check if alleles are correct
    if(x7 != 'A' and x7 != 'T' and x7 != 'C' and x7 != 'G' and x7 != 'D' and x7 != 'I'):
        print("\nIncorrect allele2. Choose from ATCGDI")
        return 0
    
    x8 = "[{a}/{b}]".format(a=x6,b=x7)
    x9 = input("Enter Sample ID\n")
    
    t = (x1,x2,x3,x4,x5,x6,x7,x8,x9) #Create a tuple from the inputed values

    return t


def search(tableName): #Function that searches for a data row by SNP Name and returns the data row
    myCursor = mydb.cursor()
    snpname = input("Enter the SNP name you are searching for\n").strip()
    sqlformula = """SELECT * FROM {a} WHERE SNP_Name ='{b}'""".format(a=tableName, b=snpname)
    
    myCursor.execute(sqlformula)
    search = myCursor.fetchone()
    return search

def filterByGC(tableName): #Function that filters results by GC Score and returns the filtered data
    print("Press 1: Print every value above this score")
    print("Press 2: Print every value below this score")
    choice = int(input("Choose an option: 1 or 2\n\n"))
    
    #Option 1
    if(choice == 1):
        myCursor = mydb.cursor()
        score = float(input("Enter the GC Score\n").strip())
    
        #Check if score is in range [0,1]
        if(score > 1):
            print("Out of range value")
            return 0
        elif(score < 0):
            print("Out of range value")
            return 0
        
        #SQL Command for selecting filtered values
        sqlf1 = """SELECT * FROM {a} WHERE GC_Score >{b}""".format(a=tableName, b=score) 
        
        myCursor.execute(sqlf1)
        filt = myCursor.fetchall()
        return filt
    
    #Option 2
    elif(choice==2):
        myCursor = mydb.cursor()
        score = float(input("Enter the GC Score\n").strip())
        
        if(score > 1):
            print("Out of range value")
            return 0
        elif(score < 0):
            print("Out of range value")
            return 0
        
        sqlf2 = """SELECT * FROM {a} WHERE GC_Score <{b}""".format(a=tableName, b=score)
    
        myCursor.execute(sqlf2)
        filt = myCursor.fetchall()
        return filt

def dropTable(tableName): #Function that drops the desired table
    answer = input("Are you sure you want to drop {} from samplesDB?\n [y/n]?\n".format(tableName))
    if(answer == 'y'):
        myCursor = mydb.cursor()
        dropcommand = "DROP TABLE {}".format(tableName)
        try:
            myCursor.execute(dropcommand)
        except:
            print("Execution failed please try again")
        print('Table deleted')
    else:
        print('\n\n')
    
def update(tableName): #Function that updates a certain data row
    print("Search for the data row you want to update. Enter below:\n\n")
    row = search(tableName)
    print('Found  \n',row)
    print("Which attribute from your data row do you want to update?\n")
    print('1: {}'.format(row[0]))
    print('2: {}'.format(float(row[1])))
    print('3: {}'.format(float(row[2])))
    print('4: {}'.format(row[3]))
    print('5: {}'.format(row[4]))
    print('6: {}'.format(row[5]))
    print('7: {}'.format(row[6]))
    print('8: {}'.format(row[7]))
    print('9: {}'.format(row[8]))
    choose = int(input("Choose the number representing the option from your keyboard\n\n"))
    if(choose == 1):
        myCursor = mydb.cursor()
        snpname = input("Enter the new value\n")
        updatecommand = "UPDATE {a} SET SNP_Name = '{b}' WHERE SNP_Name = '{c}'".format(a=tableName,b=snpname,c=row[0])
        myCursor.execute(updatecommand)
        mydb.commit()
        
    elif(choose == 2):
        myCursor = mydb.cursor()
        gc = float(input("Enter the new value\n"))
        updatecommand = "UPDATE {a} SET GC_Score = {b} WHERE SNP_Name = '{c}'".format(a=tableName,b=gc,c=row[0])
        myCursor.execute(updatecommand)
        mydb.commit()
        
    elif(choose == 3):
        myCursor = mydb.cursor()
        gt = float(input("Enter the new value\n"))
        updatecommand = "UPDATE {a} SET GT_Score = {b} WHERE SNP_Name = '{c}'".format(a=tableName,b=gt,c=row[0])
        myCursor.execute(updatecommand)
        mydb.commit()
    
    elif(choose == 4):
        myCursor = mydb.cursor()
        pos = int(input("Enter the new value\n"))
        updatecommand = "UPDATE {a} SET Position = {b} WHERE SNP_Name = '{c}'".format(a=tableName,b=pos,c=row[0])
        myCursor.execute(updatecommand)
        mydb.commit()
        
    elif(choose == 5):
        myCursor = mydb.cursor()
        chro = input("Enter the new value\n")
        updatecommand = "UPDATE {a} SET Chr = '{b}' WHERE SNP_Name = '{c}'".format(a=tableName,b=chro,c=row[0])
        myCursor.execute(updatecommand)
        mydb.commit()
    
    elif(choose == 6):
        myCursor = mydb.cursor()
        allele1 = input("Enter the new value\n")
        updatecommand = "UPDATE {a} SET Allele1Top = '{b}' WHERE SNP_Name = '{c}'".format(a=tableName,b=allele1,c=row[0])
        myCursor.execute(updatecommand)
        mydb.commit()
        
    elif(choose == 7):
        myCursor = mydb.cursor()
        allele2 = input("Enter the new value\n")
        updatecommand = "UPDATE {a} SET Allele2Top = '{b}' WHERE SNP_Name = '{c}'".format(a=tableName,b=allele2,c=row[0])
        myCursor.execute(updatecommand)
        mydb.commit()
        
    elif(choose == 8):
        myCursor = mydb.cursor()
        snp = input("Enter the new value\n")
        updatecommand = "UPDATE {a} SET SNP = '{b}' WHERE SNP_Name = '{c}'".format(a=tableName,b=snp,c=row[0])
        myCursor.execute(updatecommand)
        mydb.commit()
        
    elif(choose == 9):
        myCursor = mydb.cursor()
        sid = input("Enter the new value\n")
        updatecommand = "UPDATE {a} SET SampleID = '{b}' WHERE SNP_Name = '{c}'".format(a=tableName,b=sid,c=row[0])
        myCursor.execute(updatecommand)
        mydb.commit()
        
    else:
        print("Wrong key, it has to be a number")

def delete(tableName): #Function that deletes a certain data row
    print("Search for the row you want to delete below\n")
    row = search(tableName)
    print('found\n')
    myCursor = mydb.cursor()
    dcommand = "DELETE FROM {a} WHERE SNP_Name = '{b}'".format(a=tableName,b=row[0])
    myCursor.execute(dcommand)
    
    mydb.commit()
    print("Succesfully deleted")
    
###################################################################################

def menu():
    choice=0
    end = False
    while(end == False):
        print("Menu\n")
        print("1. Create table and fill it with data from csv file. Use with caution")
        print("2. Print the data")
        print("3. Search for row in table")
        print("4. insert row in table")
        print("5. update row in table")
        print("6. delete row in table")
        print("7. drop table")
        print("8. filter data by gc score")
        print("9. Exit")
        choice = int(input("\nPlease choose from the options above\n"))
    
        if(choice == 1):
            fileName = input("Enter the name of the csv file with full path if needed\n").strip()
            addSample(fileName)
        
        elif(choice == 2):
            tableName = input("Enter the name of the table\n").strip()
            printData(tableName)
    
        elif(choice == 3):
            tableName = input("Enter the name of the table\n").strip()
            print('\n', search(tableName))
        
        elif(choice == 4):
            tableName = input("Enter the name of the table\n").strip()
            t = getInput()
            if(t!=0):
                insert(tableName, t)
    
        elif(choice == 5):
            tableName = input("Enter the name of the table\n").strip()
            update(tableName)
        
        elif(choice == 6):
            tableName = input("Enter the name of the table\n").strip()
            delete(tableName)
        
        elif(choice == 7):
            tableName = input("Enter the name of the table\n").strip()
            dropTable(tableName)
        
        elif(choice == 8):
            tableName = input("Enter the name of the table\n").strip()
            result = filterByGC(tableName)
            for row in result:
                print('\n',row)
        
        elif(choice == 9):
            end = True
        
        else:
            print("\nThat is not an option, choose from the options above")

        
menu()