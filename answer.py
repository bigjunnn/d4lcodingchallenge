import pandas as pd
import mysql.connector as msql
from datetime import datetime, timedelta
from mysql.connector import Error
from decouple import config

data = pd.read_csv("dummy.csv")

username = config('user', default='')
userPassword = config('password', default='')


def createDatabase(username, userPassword):
    # Create Database
    try:
        conn = msql.connect(host='localhost', user=username,
                            password=userPassword)

        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE hospital")
            print("Database successfully created.")

    except Error as e:
        print("Error while connecting to MySQL", e)


def createTables(databaseName):

    try:

        if conn.is_connected():
            cursor = conn.cursor()

            # Drop tables if already exists.
            cursor.execute("DROP TABLE IF EXISTS PATIENTS")
            cursor.execute("DROP TABLE IF EXISTS DOCTORS")
            cursor.execute("DROP TABLE IF EXISTS APPOINTMENTS")

            # Creating Patients Table
            createPatientTable = '''CREATE TABLE PATIENTS(
                ID VARCHAR(255) NOT NULL,
                NAME VARCHAR(255) NOT NULL,
                AGE INT CHECK (AGE > 0),
                GENDER VARCHAR(20) NOT NULL CHECK (GENDER in ('M', 'F', 'Others')),
                PRIMARY KEY (ID)
            )'''

            # Creating Doctors Table
            createDoctorTable = '''CREATE TABLE DOCTORS(
                ID VARCHAR(255) NOT NULL,
                NAME VARCHAR(255) NOT NULL,
                PRIMARY KEY (ID)
            )'''

            # Creating Appointment Table
            createApptTable = '''CREATE TABLE APPOINTMENTS(
                ID VARCHAR(255) NOT NULL,
                PATIENT_ID VARCHAR(255) NOT NULL,
                DOCTOR_ID VARCHAR(255) NOT NULL,
                START DATETIME CHECK (TIME(START) BETWEEN '08:00:00' AND '15:00:00'),
                END DATETIME CHECK (TIME(END) BETWEEN '09:00:00' AND '16:00:00'),
                PRIMARY KEY (ID),
                FOREIGN KEY (PATIENT_ID) REFERENCES PATIENTS(ID),
                FOREIGN KEY (DOCTOR_ID) REFERENCES DOCTORS(ID)
            )'''

            cursor.execute(createPatientTable)
            print("Patients table successfully created.")

            cursor.execute(createDoctorTable)
            print("Doctors table successfully created.")

            cursor.execute(createApptTable)
            print("Appointments table successfully created.")

    except Error as e:
        print("Error while connecting to MySQL", e)


def insertPatientRecord(patient_id, name, age, gender):

    try:
        if conn.is_connected():
            cursor = conn.cursor()

            insertPatientStmt = (
                "INSERT INTO PATIENTS (ID, NAME, AGE, GENDER) "
                "VALUES (%s, %s, %s, %s);"
            )

            data = (patient_id, name, age, gender)
            cursor.execute(insertPatientStmt, data)

            conn.commit()

            print("Patient record successfully inserted.")

    except Error as e:
        pass


def insertDoctorRecord(doctor_id, name):

    try:
        if conn.is_connected():
            cursor = conn.cursor()

            insertDoctorStmt = (
                "INSERT INTO DOCTORS (ID, NAME) "
                "VALUES (%s, %s);"
            )

            data = (doctor_id, name)
            cursor.execute(insertDoctorStmt, data)

            conn.commit()

            print("Doctor record successfully inserted.")

    except Error as e:
        pass


def insertApptRecord(appt_id, patient_id, doctor_id, start_dt, end_dt):

    try:
        if conn.is_connected():
            cursor = conn.cursor()

            startDate = start_dt.date()
            startTime = start_dt.time()
            endTime = end_dt.time()

            checkConflictStmt = '''SELECT * FROM APPOINTMENTS
                WHERE (DATE(START) = %s) AND
                      (TIME(START) = %s AND TIME(END) = %s) OR
                      (TIME(START) < %s AND TIME(END) > %s) OR
                      (TIME(START) < %s AND TIME(END) > %s)                      
                LIMIT 1
            '''

            insertApptStmt = (
                "INSERT INTO APPOINTMENTS (ID, PATIENT_ID, DOCTOR_ID, START, END) "
                "VALUES (%s, %s, %s, %s, %s);"
            )

            conflictData = (startDate,
                            startTime, endTime,
                            startTime, startTime,
                            endTime, endTime)

            insertData = (appt_id, patient_id, doctor_id, start_dt, end_dt)

            cursor.execute(checkConflictStmt, conflictData)
            result = cursor.fetchall()
            if result:
                print("Unable to fix appointment due to scheduling conflicts.")
            else:
                cursor.execute(insertApptStmt, insertData)
                print("Appointment has been successfully fixed.")

            conn.commit()

    except Error as e:
        print("Error when adding appointments", e)


def createEntries(data):
    for row in data.itertuples(index=False):
        doctor_id = row[0]
        doctor_name = row[1]

        patient_id = row[2]
        patient_name = row[3]
        patient_age = row[4]
        patient_gender = row[5]

        appt_id = row[6]
        appt_start = row[7]

        datetime_str = appt_start
        appt_start_dt = datetime.strptime(datetime_str, '%d%m%Y %H:%M:%S')
        appt_end_dt = appt_start_dt + timedelta(hours=1)

        insertDoctorRecord(doctor_id, doctor_name)

        insertPatientRecord(patient_id, patient_name,
                            patient_age, patient_gender)

        insertApptRecord(appt_id, patient_id, doctor_id,
                         appt_start_dt, appt_end_dt)


# Q2
def getAppts(doctor_id, dateString):

    cursor = conn.cursor()
    dateTime_obj = datetime.strptime(dateString, '%d%m%Y')
    date = dateTime_obj.date()

    selectApptStmt = (
        "SELECT ID FROM APPOINTMENTS "
        "WHERE DOCTOR_ID = %s AND DATE(START) = %s"
    )

    data = (doctor_id, date)
    cursor.execute(selectApptStmt, data)

    result = cursor.fetchall()
    if result:
        print("Appointments found:")
        for i in result:
            print(i[0])
    else:
        print("No appointments found.")


# Q3
def fixAppt(appt_id, patient_id, doctor_id, dateTimeString):

    start = datetime.strptime(dateTimeString, '%d%m%Y %H:%M:%S')
    end = start + timedelta(hours=1)
    insertApptRecord(appt_id, patient_id, doctor_id, start, end)


# Q4
def cancelAppt(patient_id, doctor_id, dateTimeString):

    cursor = conn.cursor()
    start = datetime.strptime(dateTimeString, '%d%m%Y %H:%M:%S')
    end = start + timedelta(hours=1)

    selectApptStmt = '''SELECT ID FROM APPOINTMENTS
        WHERE   (PATIENT_ID = %s) AND
                (DOCTOR_ID = %s) AND
                (START = %s) AND
                (END = %s)
    '''

    deleteApptStmt = '''DELETE FROM APPOINTMENTS
        WHERE ID = %s 
    '''

    selectData = (patient_id, doctor_id, start, end)
    cursor.execute(selectApptStmt, selectData)
    result = cursor.fetchall()

    # Process if only 1 ID is returned
    if len(result) == 1:
        deleteData = result[0]
        cursor.execute(deleteApptStmt, deleteData)
        conn.commit()
        print("Appointment has been successfully deleted.")
    else:
        print("Appointment not found.")


# Basic set-up
createDatabase(username, userPassword)
conn = msql.connect(host='localhost', user=username,
                    password=userPassword, database='hospital')

createTables('hospital')
createEntries(data)

# Q2 Output
print('\nQ2:\n')
getAppts('D1', '08032018')  # Returns 2 entries (A1, A3)
getAppts('D2', '18032018')  # Returns 2 entries (A5, A7)
getAppts('D3', '08032018')  # Non-existent doctor
getAppts('D2', '08032022')  # Non-existent date

# Q3 Output
print('\nQ3:\n')
fixAppt('A9', 'P1', 'D1', '08032018 07:00:00')  # Before start hours
fixAppt('A9', 'P1', 'D1', '08032018 17:00:00')  # After end hours
fixAppt('A9', 'P1', 'D1', '08032018 09:00:00')  # Same existing schedule
fixAppt('A9', 'P1', 'D1', '08032018 08:45:00')  # Conflicting schedule
fixAppt('A9', 'P1', 'D1', '08032018 09:45:00')  # Conflicting schedule
fixAppt('A1', 'P1', 'D1', '08032018 11:00:00')  # Duplicate PK APPT_ID
fixAppt('A9', 'P4', 'D1', '08032018 11:00:00')  # Non-existent patient_id
fixAppt('A9', 'P1', 'D3', '08032018 11:00:00')  # Non-existent doctor_id
fixAppt('A9', 'P1', 'D1', '08032018 11:00:00')  # Valid entry

# Q4 Output
print('\nQ4:\n')
cancelAppt('P1', 'D1', '08032018 09:45:00')  # Invalid appointment
cancelAppt('P1', 'D1', '08032018 11:00:00')  # Valid appointment (A9 deleted)
