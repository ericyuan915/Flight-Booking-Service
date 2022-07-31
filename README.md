# Flight-Booking-Service

# Database Application and Transaction Management

**tools:**
* [SQLite](https://www.sqlite.org/index.html) through [Python apsw library](https://rogerbinns.github.io/apsw/index.html)
* starter code files

## Details
* Design a database of customers and the flights they book
* Complete a working prototype of flight booking application that connects to the SQLite database then allows customers to use a CLI to search, book, cancel, etc. flights

UI (`FlightService.py`) and partial backend (`Query.py`).
In real life, I would develop a web-based interface instead of a CLI.


*** Please enter one of the following commands ***
> create <username> <password> <initial amount>
> login <username> <password>
> search <origin city> <destination city> <direct> <day> <num itineraries>
> book <itinerary id>
> pay <reservation id>
> reservations
> cancel <reservation id>
> quit
```

#### Data Model

The flight service system consists of the following logical entities.
These entities are *not necessarily database tables*.
We have provided a database scheme for you which is available in `create_tables.sql`. You can check the schema design.

- **Flights / Carriers / Months / Weekdays**

- **Users**: A user has a username (`varchar`), password (`varbinary`), and balance (`int`) in their account.
  All usernames should be unique in the system. Each user can have any number of reservations.
  Usernames are case insensitive (this is the default for SQLite).
  You can assume that all usernames and passwords have at most 20 characters.
  For implementation simplicity, we do not encrypt the password, which is quite `dangerous` in real-world applications.

- **Itineraries**: An itinerary is either a direct flight (consisting of one flight: origin --> destination) or
  a one-hop flight (consisting of two flights: origin --> stopover city, stopover city --> destination). Itineraries are returned by the search command.

- **Reservations**: A booking for an itinerary, which may consist of one (direct) or two (one-hop) flights.
  Each reservation can either be paid or unpaid, cancelled or not, and has a unique ID.

- **ReservationsId**: Used for obtaining atomically increasing reservation ID.


#### Requirements
The following are the functional specifications for the flight service system, implemented in `Query.py`

- **create** takes in a new username, password, and initial account balance as input. It creates a new user account with the initial balance.
  It should return an error if negative, or if the username already exists. Usernames are checked case-insensitively.
  You can assume that all usernames and passwords have at most 20 characters.

- **login** takes in a username and password, and checks that the user exists in the database and that the password matches. To compute the hash, adapt the above code.
  Within a single session (that is, a single instance of your program), only one user should be logged in. You can track this via a local variable in your program.
  If a second login attempt is made, please return "User already logged in".
  Across multiple sessions (that is, if you run your program multiple times), the same user is allowed to be logged in.
  This means that you do not need to track a user's login status inside the database.

- **search** takes as input an origin city (string), a destination city (string), a flag for only direct flights or not (0 or 1), the date (int), and the maximum number of itineraries to be returned (int).
  For the date, we only need the day of the month, since our dataset comes from July 2015. Return only flights that are not canceled, ignoring the capacity and number of seats available.
  If the user requests n itineraries to be returned, there are a number of possibilities:
    * direct=1: return up to n direct itineraries
    * direct=0: return up to n direct itineraries. If there are only k direct itineraries (where k < n), find the k direct itineraries and up to (n-k) of the shortest indirect itineraries with the flight times. Then sort the combinations of direct and indirect flights purely based on the total travel time. For one-hop flights, different carriers can be used for the flights. For the purpose of this assignment, an indirect itinerary means the first and second flight only must be on the same date (i.e., if flight 1 runs on the 3rd day of July, flight 2 runs on the 4th day of July, then you can't put these two flights in the same itinerary as they are not on the same day).

  <br />Sort results. In all cases, the returned results should be primarily sorted on total actual_time (ascending. There could be some indirect flights have less total travel time less than the direct flight). If a tie occurs, break that tie by the fid value. Use the first then the second fid for tie-breaking.

    Below is an example of a single direct flight from Seattle to Boston. Actual itinerary numbers might differ, notice that only the day is printed out since we assume all flights happen in July 2015:

    ```
    Itinerary 0: 1 flight(s), 297 minutes
    ID: 60454 Day: 1 Carrier: AS Number: 24 Origin: Seattle WA Dest: Boston MA Duration: 297 Capacity: 14 Price: 140
    ```

    Below is an example of two indirect flights from Seattle to Boston:

    ```
    Itinerary 0: 2 flight(s), 317 minutes
    ID: 704749 Day: 10 Carrier: AS Number: 16 Origin: Seattle WA Dest: Orlando FL Duration: 159 Capacity: 10 Price: 494
    ID: 726309 Day: 10 Carrier: B6 Number: 152 Origin: Orlando FL Dest: Boston MA Duration: 158 Capacity: 0 Price: 104
    Itinerary 1: 2 flight(s), 317 minutes
    ID: 704749 Day: 10 Carrier: AS Number: 16 Origin: Seattle WA Dest: Orlando FL Duration: 159 Capacity: 10 Price: 494
    ID: 726464 Day: 10 Carrier: B6 Number: 452 Origin: Orlando FL Dest: Boston MA Duration: 158 Capacity: 7 Price: 760
    ```

    Note that for one-hop flights, the results are printed in the order of the itinerary, starting from the flight leaving the origin and ending with the flight arriving at the destination.

    The returned itineraries should start from 0 and increase by 1 up to n as shown above. If no itineraries match the search query, the system should return an informative error message. See `Query.py` for the actual text.

    The user need not be logged in to search for flights.

    All flights in an indirect itinerary should be under the same itinerary ID. In other words, the user should only need to book once with the itinerary ID for direct or indirect trips.


- **book** lets a user book an itinerary by providing the itinerary number as returned by a previous search.
  The user must be logged in to book an itinerary, and must enter a valid itinerary id that was returned in the last search that was performed *within the same login session*.
  Make sure you make the corresponding changes to the tables in case of a successful booking. Once the user logs out (by quitting the application),
  logs in (if they previously were not logged in), or performs another search within the same login session,
  then all previously returned itineraries are invalidated and cannot be booked.

  A user cannot book a flight if the flight's maximum capacity would be exceeded. Each flight’s capacity is stored in the Flights table, and you should have records as to how many seats remain on each flight based on the reservations.

  If booking is successful, then assign a new reservation ID to the booked itinerary.
  Note that 1) each reservation can contain up to 2 flights (in the case of indirect flights),
  and 2) each reservation should have a unique ID that incrementally increases by 1 for each successful booking.


- **pay** allows a user to pay for an existing unpaid reservation.
  It first checks whether the user has enough money to pay for all the flights in the given reservation. If successful, it updates the reservation to be paid.


- **reservations** lists all reservations for the currently logged-in user.
  Each reservation must have ***a unique identifier (which is different for each itinerary) in the entire system***, starting from 1 and increasing by 1 after each reservation is made.

  There are many ways to implement this. One possibility is to define a "ID" table that stores the next ID to use, and update it each time when a new reservation is made successfully.

  The user must be logged in to view reservations. The itineraries should be displayed using similar format as that used to display the search results, and they should be shown in increasing order of reservation ID under that username.
  Cancelled reservations should not be displayed.


- **cancel** lets a user to cancel an existing uncanceled reservation. The user must be logged in to cancel reservations and must provide a valid reservation ID.
  Make sure you make the corresponding changes to the tables in case of a successful cancellation (e.g., if a reservation is already paid, then the customer should be refunded).


- **quit** leaves the interactive system and logs out the current user (if logged in).


#### Testing:

The test harness will run all the test cases in the provided `testcases/` folder. To run the harness, execute `python3 grading.py`.

#### Python customer application

### Step 0: Install dependencies
1. pip3 install -r requirements.txt
2. install sqlite3 on your machine
3. tar -zxvf flights-small.tar.gz
4. git clone the repository
5. run `python3 grading.py`

