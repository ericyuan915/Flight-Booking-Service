import sqlite3
import operator
import subprocess
import os
import csv
import apsw
import time
import threading

DB_NAME = "example.db"


# A class to store flight information.
class Flight:
    def __init__(self, fid=-1, dayOfMonth=0, carrierId=0, flightNum=0, originCity="", destCity="", time=0, capacity=0,
                 price=0):
        self.fid = fid
        self.dayOfMonth = dayOfMonth
        self.carrierId = carrierId
        self.flightNum = flightNum
        self.originCity = originCity
        self.destCity = destCity
        self.time = time
        self.capacity = capacity
        self.price = price

    def toString(self):
        return "ID: {} Day: {} Carrier: {} Number: {} Origin: {} Dest: {} Duration: {} Capacity: {} Price: {}\n".format(
            self.fid, self.dayOfMonth, self.carrierId, self.flightNum, self.originCity, self.destCity, self.time,
            self.capacity, self.price)


class Itinerary:
    # one-hop flight
    def __init__(self, time, flight1=Flight(), flight2=Flight()):  # the second one could be empty flight
        self.flights = []
        self.flights.append(flight1)
        self.flights.append(flight2)
        self.flight1 = flight1
        self.flight2 = flight2
        self.time = time

    def itineraryPrice(self):
        price = 0
        for f in self.flights:
            price += f.price
        return price

    def numFlights(self):
        if (self.flights[1].fid == -1):
            return 1
        else:
            return 2

    def response(self, index):
        response = ""
        if self.flight2.fid != -1:
            response += """Itinerary {}: 2 flight(s), {} minutes\n""".format(index, self.time)
            response += self.flight1.toString()
            response += self.flight2.toString()
        else:
            response += """Itinerary {}: 1 flight(s), {} minutes\n""".format(index, self.time)
            response += self.flight1.toString()
        return response


class Query:
    CREATE_CUSTOMER_SQL = "INSERT INTO Customers VALUES('{}', '{}', {})"
    CHECK_USER = "SELECT * FROM Customers c WHERE c.username = '{}' and c.password = '{}'"
    CHECK_ITINERARY = "select F.fid, F.day_of_month, F.carrier_id, F.flight_num, F.origin_city, F.dest_city, F.actual_time, F.capacity, F.price from Flights F where F.origin_city = '{}' and F.dest_city = '{}' and F.day_of_month = '{}' and F.canceled = 0 order by F.actual_time,fid limit {}"
    CHECK_ALL_FLIGHTS = "select F.fid, F.day_of_month, F.carrier_id, F.flight_num, F.origin_city, F.dest_city, F.actual_time, F.capacity, F.price, null as empty1,null as empty2,null as empty3,null as empty4,null as empty5,null as empty6,null as empty7,null as empty8,null as empty9, F.actual_time as total_time from Flights F where F.origin_city = '{}' and F.dest_city = '{}' and F.day_of_month = '{}' and F.canceled = 0 union select A.fid, A.day_of_month, A.carrier_id, A.flight_num, A.origin_city, A.dest_city, A.actual_time, A.capacity, A.price, B.fid as Bfid, B.day_of_month as Bday_of_month, B.carrier_id as Bcarrier_id, B.flight_num as Bflight_num, B.origin_city as Borigin_city, B.dest_city as Bdest_city, B.actual_time as Bactual_time, B.capacity as Bcapacity, B.price as Bprice, (A.actual_time + B.actual_time) as total_time from Flights A, Flights B where A.origin_city = '{}' and B.dest_city = '{}' and A.dest_city = B.origin_city and A.day_of_month = '{}' and B.day_of_month = '{}' and A.canceled = 0 and B.canceled = 0 order by total_time,A.fid,B.fid limit {};"
    INSERT_INTO_RESERVATIONS = "insert into Reservations values({}, {}, {}, {}, 0, 0, '{}', {})"
    UPDATE_NEXT_ID = "update ReservationsId SET rid = {}"

    CHECK_UNPAID_RESV = "select price from Reservations where rid = {} and username = '{}' and paid = 0"
    # CHECK_RESV_USERNAME = "select username from Reservations where rid = {}"
    CHECK_BALANCE = "select balance from Customers where username = '{}'"
    UPDATE_BAL = "update Customers SET balance = {} WHERE username = '{}'"
    UPDATE_RES_PAID = "update Reservations set paid = 1 where rid = {} and username ='{}'"
    CHECK_USER_RESV = "select rid, price, fid1, fid2, paid from Reservations where username = '{}'"
    # CHECK_PAY_RESV = "select rid from Reservations where username = '{}'"
    GET_FLIGHT_INFO = "select F.fid, F.day_of_month, F.carrier_id, F.flight_num, F.origin_city, F.dest_city, F.actual_time, F.capacity, F.price from Flights F where F.fid = '{}'"

    GET_AVAILABLE_RID = "select rid from ReservationsId"
    CHECK_FLIGHT_DAY = "SELECT * FROM Reservations r, Flights f WHERE r.username = '{}' AND f.day_of_month = {} AND r.fid1 = f.fid"
    CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = {}"
    CHECK_BOOKED_SEATS = "SELECT COUNT(*) AS cnt FROM Reservations WHERE fid1 = {} or fid2 = {}"
    CLEAR_DB_SQL1 = "DELETE FROM Reservations;"
    CLEAR_DB_SQL2 = "DELETE FROM Customers;"
    CLEAR_DB_SQL3 = "UPDATE ReservationsId SET rid = 1;"

    FIND_RESV = "select * from Reservations where rid = '{}' and username = '{}'"
    CANCEL_RESV = "delete from Reservations where rid = '{}' and username = '{}'"

    username = None
    lastItineraries = []

    def __init__(self):
        self.conn = None
        self.db_name = DB_NAME
        self.conn = apsw.Connection(self.db_name, statementcachesize=0)
        self.conn.setbusytimeout(5000)

    def startConnection(self):
        # if self.conn is None:
        #     self.conn = apsw.Connection(self.db_name, statementcachesize=0)
        #     self.conn.setbusytimeout(5000)
        self.conn = apsw.Connection(self.db_name, statementcachesize=0)

    def closeConnection(self):
        # if self.conn is not None:
        #     self.conn.close()
        #     self.conn = None
        self.conn.close()

    '''
    * Clear the data in any custom tables created. and reload the Carriers, Flights, Weekdays and Months tables.
    * 
    * WARNING! Do not drop any tables and do not clear the flights table.
    '''

    def clearTables(self):
        try:
            os.remove(DB_NAME)
            open(DB_NAME, 'w').close()
            os.system("chmod 777 {}".format(DB_NAME))
        # except PermissionError:
        #     print("PermissionError")

        # try:
            # self.startConnection()
            # remove old db file
            # TODO use sqlite3 example.db < create_tables.sql to reconstruct the db file. This can save many lines of code.
            # I have to reconstruct the db before each test
            self.conn = apsw.Connection(self.db_name, statementcachesize=0)

            self.conn.cursor().execute("PRAGMA foreign_keys=ON;")
            self.conn.cursor().execute(" PRAGMA serializable = true;")
            self.conn.cursor().execute("CREATE TABLE Carriers (cid VARCHAR(7) PRIMARY KEY, name VARCHAR(83))")
            self.conn.cursor().execute("""   
                    CREATE TABLE Months (
                        mid INT PRIMARY KEY,
                        month VARCHAR(9)
                    );""")

            self.conn.cursor().execute("""       
                    CREATE TABLE Weekdays(
                        did INT PRIMARY KEY,
                        day_of_week VARCHAR(9)
                    );""")
            self.conn.cursor().execute("""      
                    CREATE TABLE Flights (
                        fid INT PRIMARY KEY, 
                        month_id INT,        -- 1-12
                        day_of_month INT,    -- 1-31 
                        day_of_week_id INT,  -- 1-7, 1 = Monday, 2 = Tuesday, etc
                        carrier_id VARCHAR(7), 
                        flight_num INT,
                        origin_city VARCHAR(34), 
                        origin_state VARCHAR(47), 
                        dest_city VARCHAR(34), 
                        dest_state VARCHAR(46), 
                        departure_delay INT, -- in mins
                        taxi_out INT,        -- in mins
                        arrival_delay INT,   -- in mins
                        canceled INT,        -- 1 means canceled
                        actual_time INT,     -- in mins
                        distance INT,        -- in miles
                        capacity INT, 
                        price INT,           -- in $
                        FOREIGN KEY (carrier_id) REFERENCES Carriers(cid),
                        FOREIGN KEY (month_id) REFERENCES Months(mid),
                        FOREIGN KEY (day_of_week_id) REFERENCES Weekdays(did)
                    );""")
            self.conn.cursor().execute("""        
                    CREATE TABLE Customers(
                        username VARCHAR(256),
                        password VARCHAR(256),
                        balance INT,
                        PRIMARY KEY (username)
                    );""")
            self.conn.cursor().execute("""      
                    CREATE TABLE Itineraries(
                        direct INT, -- 1 or 0 stands for direct or one-hop flights
                        fid1 INT,
                        fid2 INT -- -1 means that this is a direct flight and has no second flight
                    );""")
            self.conn.cursor().execute("""      
                    CREATE TABLE Reservations(
                        rid INT,
                        price INT,
                        fid1 INT,
                        fid2 INT,
                        paid INT,
                        canceled INT,
                        username VARCHAR(256),
                        day_of_month INT,
                        PRIMARY KEY (rid)
                    );""")
            self.conn.cursor().execute("""      
                    CREATE TABLE ReservationsId(
                        rid INT
                    );""")

            self.conn.cursor().execute("INSERT INTO ReservationsId VALUES (1);")

            # reload db file for next tests

            with open("carriers.csv") as carriers:
                carriers_data = csv.reader(carriers)
                self.conn.cursor().executemany("INSERT INTO Carriers VALUES (?, ?)", carriers_data)

            with open("months.csv") as months:
                months_data = csv.reader(months)
                self.conn.cursor().executemany("INSERT INTO Months VALUES (?, ?)", months_data)

            with open("weekdays.csv") as weekdays:
                weekdays_data = csv.reader(weekdays)
                self.conn.cursor().executemany("INSERT INTO Weekdays VALUES (?, ?)", weekdays_data)

            # conn.cursor().executemany() is too slow to load largecsv files... so i use the command line instead for flights.csv
            subprocess.run(['sqlite3',
                            "example.db",
                            '-cmd',
                            '.mode csv',
                            '.import flights-small.csv Flights'])

        except sqlite3.Error:
            print("clear table SQL execution meets Error")
        # else:
        #     self.closeConnection()

    '''
   * Implement the create user function.
   *
   * @param username   new user's username. User names are unique the system.
   * @param password   new user's password.
   * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure
   *                   otherwise).
   *
   * @return either "Created user `username`\n" or "Failed to create user\n" if failed.
    '''

    def transactionCreateCustomer(self, username, password, initAmount):
        # this is an example function.
        # can = False
        # while not can:
        #     try:
        self.conn.cursor().execute("BEGIN EXCLUSIVE;")
            #     can = True
            # except:
            #     can = False
        # self.conn.cursor().execute("BEGIN EXCLUSIVE;")
        response = ""
        try:
            # self.startConnection()
            if (initAmount >= 0):
                self.conn.cursor().execute(self.CREATE_CUSTOMER_SQL.format(username, password, initAmount))
                response = "Created user {}\n".format(username)
                self.conn.cursor().execute("COMMIT;")
            else:
                response = "Failed to create user\n"
                self.conn.cursor().execute("ROLLBACK;")
        except apsw.ConstraintError:
            # we already have this customer. we can not create it again
            # print("create user meets apsw.ConstraintError")
            response = "Failed to create user\n"
            self.conn.cursor().execute("ROLLBACK;")
        return response

    '''
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username user's username
   * @param password user's password
   *
   * @return If someone has already logged in, then return "User already logged in\n" For all other
   *         errors, return "Login failed\n". Otherwise, return "Logged in as [username]\n".
    '''

    def transactionLogin(self, username, password):
        # TODO your code here
        response = ""
        result = self.conn.cursor().execute(self.CHECK_USER.format(username, password)).fetchone()
        # print('error',result)
        try:
            if (self.username != None):
                response = "User already logged in\n"
            else:
                if (result != None):
                    self.username = username
                    response = "Logged in as {}\n".format(username)
                else:
                    response = "Login failed\n"
        except apsw.Error:
            response = "Login failed\n"
        return response

    # def test(self, originCity, destCity, directFlight, dayOfMonth, numberOfItineraries):
    #     fli = Flight(self.conn.cursor().execute(
    #         self.CHECK_ITINERARY.format(originCity, destCity, dayOfMonth, numberOfItineraries)).fetchone())
    #     print(fli)

    '''
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination city, on the given day
   * of the month. If {@code directFlight} is true, it only searches for direct flights, otherwise
   * is searches for direct flights and flights with two "hops." Only searches for up to the number
   * of itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight        if true, then only search for direct flights, otherwise include
   *                            indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n". If an error
   *         occurs, then return "Failed to search\n".
   *
   *         Otherwise, the sorted itineraries printed in the following format:
   *
   *         Itinerary [itinerary number]: [number of flights] flight(s), [total flight time]
   *         minutes\n [first flight in itinerary]\n ... [last flight in itinerary]\n
   *
   *         Each flight should be printed using the same format as in the {@code Flight} class.
   *         Itinerary numbers in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   '''

    def transactionSearch(self, originCity, destCity, directFlight, dayOfMonth, numberOfItineraries):
        self.lastItineraries = []
        response = ""
        direct_flights = list(self.conn.cursor().execute(
            self.CHECK_ITINERARY.format(originCity, destCity, dayOfMonth, numberOfItineraries)).fetchall())
        direct_flights_count = len(direct_flights)
        all_flights = list(self.conn.cursor().execute(
            self.CHECK_ALL_FLIGHTS.format(originCity, destCity, dayOfMonth, originCity, destCity, dayOfMonth,
                                          dayOfMonth, numberOfItineraries)).fetchall())
        all_flights_count = len(all_flights)
        itineraryNum = 0
        try:
            if direct_flights_count == 0 and all_flights_count == 0:
                response = "No flights match your selection\n"
            else:
                if directFlight == 1:
                    if direct_flights_count == 0:
                        response = "No flights match your selection\n"
                    else:
                        for itinerary in direct_flights:
                            fli = Flight(itinerary[0], itinerary[1], itinerary[2], itinerary[3], itinerary[4],
                                         itinerary[5], itinerary[6], itinerary[7], itinerary[8])
                            response += ("""Itinerary {}: 1 flight(s), {} minutes\n""").format(itineraryNum,
                                                                                               itinerary[6])
                            response += fli.toString()
                            self.lastItineraries.append([itineraryNum, fli, -2])
                            itineraryNum += 1
                elif directFlight == 0:
                    if all_flights_count == 0:
                        response = "No flights match your selection\n"
                    elif direct_flights_count >= numberOfItineraries:
                        for itinerary in direct_flights:
                            fli = Flight(itinerary[0], itinerary[1], itinerary[2], itinerary[3], itinerary[4],
                                         itinerary[5], itinerary[6], itinerary[7], itinerary[8])
                            response += ("""Itinerary {}: 1 flight(s), {} minutes\n""").format(itineraryNum,
                                                                                               itinerary[6])
                            self.lastItineraries.append([itineraryNum, fli, -2])
                            itineraryNum += 1
                            response += fli.toString()
                    else:
                        itinerary_list = []
                        two_flights_itinerary_list = []

                        for direct_f in direct_flights:
                            flight = Flight(direct_f[0], direct_f[1], direct_f[2], direct_f[3], direct_f[4],
                                            direct_f[5],
                                            direct_f[6], direct_f[7], direct_f[8])
                            itinerary_list.append(Itinerary(direct_f[6], flight))
                       
                        remain_count = numberOfItineraries - direct_flights_count

                        for all_itinerary in all_flights:
                            if all_itinerary[11] != None:
                                fli1 = Flight(all_itinerary[0], all_itinerary[1], all_itinerary[2], all_itinerary[3],
                                              all_itinerary[4], all_itinerary[5], all_itinerary[6], all_itinerary[7],
                                              all_itinerary[8])
                                fli2 = Flight(all_itinerary[9], all_itinerary[10], all_itinerary[11], all_itinerary[12],
                                              all_itinerary[13], all_itinerary[14], all_itinerary[15],
                                              all_itinerary[16], all_itinerary[17])
                                two_flights_itinerary_list.append(Itinerary(all_itinerary[18], fli1, fli2))

                        # two_flights_itinerary_list.sort(key=lambda x: x[5])
                        non_direct_f_list = two_flights_itinerary_list[:remain_count]
                        for f in non_direct_f_list:
                            itinerary_list.append(f)

                        cmpfun2 = operator.attrgetter('time')
                        itinerary_list.sort(key=cmpfun2, reverse=False)

                        for i in itinerary_list:
                            if (i.flight2.fid != -1):
                                self.lastItineraries.append([itineraryNum,i.flight1, i.flight2])
                            else:
                                self.lastItineraries.append([itineraryNum,i.flight1, -2])
                        
                            response += i.response(itineraryNum)
                            itineraryNum += 1


        except:
            response = "Failed to search\n"

        return response


    '''
    * Implements the book itinerary function.
    *
    * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in
    *                    the current session.
    *
    * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
    *         If the user is trying to book an itinerary with an invalid ID or without having done a
    *         search, then return "No such itinerary {@code itineraryId}\n". If the user already has
    *         a reservation on the same day as the one that they are trying to book now, then return
    *         "You cannot book two flights in the same day\n". For all other errors, return "Booking
    *         failed\n".
    *
    *         And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n"
    *         where reservationId is a unique number in the reservation system that starts from 1 and
    *         increments by 1 each time a successful reservation is made by any user in the system.
    '''

    def transactionBook(self, itineraryId):
        self.conn.cursor().execute("BEGIN EXCLUSIVE;")
        # can = False
        # while not can:
        #     try:
        #         self.conn.cursor().execute("BEGIN EXCLUSIVE;")
        #         can = True
        #     except:
        #         can = False
        response = ""
        # length = len(self.lastItineraries)
        valid_id = False
        # print(self.lastItineraries[0][1].fid)
        try:
            if (self.username == None):
                response = "Cannot book reservations, not logged in\n"
                self.conn.cursor().execute("ROLLBACK;")
            else:
                for it in self.lastItineraries:
                    if itineraryId == it[0]:
                        valid_id = True
                if valid_id == False:
                    response = "No such itinerary {}\n".format(itineraryId)
                    self.conn.cursor().execute("ROLLBACK;")
                else:
                    if self.lastItineraries[itineraryId][2] != -2:
                        f_id_1 = self.lastItineraries[itineraryId][1].fid
                        f_id_2 = self.lastItineraries[itineraryId][2].fid
                        p = self.lastItineraries[itineraryId][1].price + self.lastItineraries[itineraryId][2].price
                        day = self.lastItineraries[itineraryId][1].dayOfMonth
                        if self.checkFlightSameDay(self.username,day):
                            response = "You cannot book two flights in the same day\n"
                            self.conn.cursor().execute("ROLLBACK;")
                        else:
                            if self.checkFlightIsFull(f_id_1) or self.checkFlightIsFull(f_id_2):
                                response = "Booking failed\n"
                                self.conn.cursor().execute("ROLLBACK;")
                            else:
                                reservationId = list(self.conn.cursor().execute(self.GET_AVAILABLE_RID))[0][0]
                                next_id = reservationId + 1
                                self.conn.cursor().execute(
                                    self.INSERT_INTO_RESERVATIONS.format(reservationId, p, f_id_1, f_id_2, self.username,
                                                                        day))
                                self.conn.cursor().execute(self.UPDATE_NEXT_ID.format(next_id))
                                response = "Booked flight(s), reservation ID: {}\n".format(reservationId)
                                self.conn.cursor().execute("COMMIT;")
                    else:
                        f_id_1 = self.lastItineraries[itineraryId][1].fid
                        p = self.lastItineraries[itineraryId][1].price
                        day = self.lastItineraries[itineraryId][1].dayOfMonth
                        if self.checkFlightSameDay(self.username,day):
                            response = "You cannot book two flights in the same day\n"
                            self.conn.cursor().execute("ROLLBACK;")
                        else:
                            if self.checkFlightIsFull(f_id_1):
                                response = "Booking failed\n"
                                self.conn.cursor().execute("ROLLBACK;")
                            else:
                                reservationId = list(self.conn.cursor().execute(self.GET_AVAILABLE_RID))[0][0]
                                next_id = reservationId + 1
                                self.conn.cursor().execute(
                                    self.INSERT_INTO_RESERVATIONS.format(reservationId, p, f_id_1, -1, self.username, day))
                                self.conn.cursor().execute(self.UPDATE_NEXT_ID.format(next_id))
                                response = "Booked flight(s), reservation ID: {}\n".format(reservationId)
                                self.conn.cursor().execute("COMMIT;")
        except:
            response = "Booking failed\n"
            self.conn.cursor().execute("ROLLBACK;")
        return response


    '''
    * Implements the pay function.
    *
    * @param reservationId the reservation to pay for.
    *
    * @return If no user has logged in, then return "Cannot pay, not logged in\n" If the reservation
    *         is not found / not under the logged in user's name, then return "Cannot find unpaid
    *         reservation [reservationId] under user: [username]\n" If the user does not have enough
    *         money in their account, then return "User has only [balance] in account but itinerary
    *         costs [cost]\n" For all other errors, return "Failed to pay for reservation
    *         [reservationId]\n"
    *
    *         If successful, return "Paid reservation: [reservationId] remaining balance:
    *         [balance]\n" where [balance] is the remaining balance in the user's account.
    '''


    def transactionPay(self, reservationId):
        self.conn.cursor().execute("BEGIN EXCLUSIVE;")
        # can = False
        # while not can:
        #     try:
        #         self.conn.cursor().execute("BEGIN EXCLUSIVE;")
        #         can = True
        #     except:
        #         can = False
        response = ""
        try:
            if (self.username == None):
                response = "Cannot pay, not logged in\n"
                self.conn.cursor().execute("ROLLBACK;")
            else:
                # try:
                price = list(self.conn.cursor().execute(self.CHECK_UNPAID_RESV.format(reservationId, self.username)))
                found_or_not = len(price)
                if found_or_not == 0:
            # except:
                    response = "Cannot find unpaid reservation {} under user: {}\n".format(reservationId,
                                                                                         self.username)
                    self.conn.cursor().execute("ROLLBACK;")
                # if (found_or_not == 0):
                #     response = "Cannot find unpaid reservation {} under user: '{}'\n".format(reservationId,
                #                                                                              self.username)
                # else:
                # print('price',price)
                else:
                    price1 = price[0][0]
                    bal = list(self.conn.cursor().execute(self.CHECK_BALANCE.format(self.username)).fetchone())[0]
                    if bal < price1:
                        response = "User has only {} in account but itinerary costs {}\n".format(bal, price1)
                        self.conn.cursor().execute("ROLLBACK;")
                    else:
                        self.conn.cursor().execute(self.UPDATE_BAL.format((bal - price1), self.username))
                        self.conn.cursor().execute(self.UPDATE_RES_PAID.format(reservationId, self.username))
                        response = "Paid reservation: {} remaining balance: {}\n".format(reservationId, (bal - price1))
                        self.conn.cursor().execute("COMMIT;")
        except:
            response = "Failed to pay for reservation {}\n".format(reservationId)
            self.conn.cursor().execute("ROLLBACK;")

        return response


    '''
    * Implements the reservations function.
    *
    * @return If no user has logged in, then return "Cannot view reservations, not logged in\n" If
    *         the user has no reservations, then return "No reservations found\n" For all other
    *         errors, return "Failed to retrieve reservations\n"
    *
    *         Otherwise return the reservations in the following format:
    *
    *         Reservation [reservation ID] paid: [true or false]:\n [flight 1 under the
    *         reservation]\n [flight 2 under the reservation]\n Reservation [reservation ID] paid:
    *         [true or false]:\n [flight 1 under the reservation]\n [flight 2 under the
    *         reservation]\n ...
    *
    *         Each flight should be printed using the same format as in the {@code Flight} class.
    *
    * @see Flight#toString()
    '''


    def transactionReservation(self):
        # can = False
        # while not can:
        #     try:
        #         self.conn.cursor().execute("BEGIN EXCLUSIVE;")
        #         can = True
        #     except:
        #         can = False
        # self.conn.cursor().execute("BEGIN EXCLUSIVE;")
        response = ""
        try:
            if (self.username == None):
                response = "Cannot view reservations, not logged in\n"
                # self.conn.cursor().execute("ROLLBACK;")
            else:
                self.conn.cursor().execute("BEGIN EXCLUSIVE;")
                resv = list(self.conn.cursor().execute(self.CHECK_USER_RESV.format(self.username)).fetchall())
                self.conn.cursor().execute("COMMIT;")
                found_length = len(resv)
                if found_length == 0:
                    response = "No reservations found\n"
                    # self.conn.cursor().execute("ROLLBACK;")
                else:
                    for re in resv:
                        if re[4] == 0:
                            pay_or_not = "false"
                        else:
                            pay_or_not = "true"
                        response += "Reservation {} paid: {}:\n".format(re[0], pay_or_not)
                        if re[3] == -1:
                            f_info = list(self.conn.cursor().execute(self.GET_FLIGHT_INFO.format(re[2])).fetchone())
                            fli = Flight(f_info[0], f_info[1], f_info[2], f_info[3], f_info[4], f_info[5], f_info[6],
                                         f_info[7], f_info[8])
                            response += fli.toString()
                            # self.conn.cursor().execute("COMMIT;")
                        else:
                            f_info_1 = list(self.conn.cursor().execute(self.GET_FLIGHT_INFO.format(re[2])).fetchone())
                            flight_1 = Flight(f_info_1[0], f_info_1[1], f_info_1[2], f_info_1[3], f_info_1[4],
                                              f_info_1[5], f_info_1[6], f_info_1[7], f_info_1[8])
                            response += flight_1.toString()

                            f_info_2 = list(self.conn.cursor().execute(self.GET_FLIGHT_INFO.format(re[3])).fetchone())
                            flight_2 = Flight(f_info_2[0], f_info_2[1], f_info_2[2], f_info_2[3], f_info_2[4],
                                              f_info_2[5], f_info_2[6], f_info_2[7], f_info_2[8])
                            response += flight_2.toString()
                            # self.conn.cursor().execute("COMMIT;")


        except:
            response = "Failed to retrieve reservations\n"
            # self.conn.cursor().execute("ROLLBACK;")

        # self.conn.cursor().execute("COMMIT;")
        return response


    '''
    * Implements the cancel operation.
    *
    * @param reservationId the reservation ID to cancel
    *
    * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n" For
    *         all other errors, return "Failed to cancel reservation [reservationId]\n"
    *
    *         If successful, return "Canceled reservation [reservationId]\n"
    *
    *         Even though a reservation has been canceled, its ID should not be reused by the system.
    '''


    def transactionCancel(self, reservationId):
        # self.conn.cursor().execute("BEGIN EXCLUSIVE;")
        can = False
        while not can:
            try:
                self.conn.cursor().execute("BEGIN EXCLUSIVE;")
                can = True
            except:
                can = False
        response = ""
        try:
            if (self.username == None):
                response = "Cannot cancel reservations, not logged in\n"
                self.conn.cursor().execute("ROLLBACK;")
            else:
                resv = list(self.conn.cursor().execute(self.FIND_RESV.format(reservationId,self.username)))
                found_length = len(resv)
                if found_length == 0:
                    response = "Failed to cancel reservation {}\n".format(reservationId)
                    self.conn.cursor().execute("ROLLBACK;")
                else:
                    if resv[0][4] == 0:
                        self.conn.cursor().execute(self.CANCEL_RESV.format(reservationId,self.username))
                        response = "Canceled reservation {}\n".format(reservationId)
                        self.conn.cursor().execute("COMMIT;")
                    else:
                        refund = resv[0][1]
                        leftover = list(self.conn.cursor().execute(self.CHECK_BALANCE.format(self.username)).fetchone())[0]
                        newBalance = refund + leftover
                        self.conn.cursor().execute(self.UPDATE_BAL.format(newBalance,self.username))
                        self.conn.cursor().execute(self.CANCEL_RESV.format(reservationId,self.username))
                        response = "Canceled reservation {}\n".format(reservationId)
                        self.conn.cursor().execute("COMMIT;")

        except:
            response = "Failed to cancel reservation {}\n".format(reservationId)
            self.conn.cursor().execute("ROLLBACK;")

        return response


    '''
    Example utility function that uses prepared statements
    '''


    def checkFlightCapacity(self, fid):
        result = self.conn.cursor().execute(self.CHECK_FLIGHT_CAPACITY.format(fid)).fetchone()
        if (result != None):
            return result[0]
        else:
            return 0


    def checkFlightIsFull(self, fid):
        capacity = self.conn.cursor().execute(self.CHECK_FLIGHT_CAPACITY.format(fid)).fetchone()[0]
        booked_seats = self.conn.cursor().execute(self.CHECK_BOOKED_SEATS.format(fid, fid)).fetchone()[0]
        # print("Checking booked/capacity {}/{}".format(booked_seats, capacity))
        return booked_seats >= capacity


    def checkFlightSameDay(self, username, dayOfMonth):
        result = self.conn.cursor().execute(self.CHECK_FLIGHT_DAY.format(username, dayOfMonth)).fetchall()
        if (len(result) == 0):
            # have not found there are multiple flights on the specific day by current user.
            return False
        else:
            return True
