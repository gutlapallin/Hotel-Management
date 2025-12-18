/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science  &  Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;


/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */
public class DBProject {

   // reference to physical database connection.
   private Connection _connection = null;

   // handling the keyboard inputs through a BufferedReader
   // This variable can be global for convenience.
   static BufferedReader in = new BufferedReader(
                                new InputStreamReader(System.in));

   /**
    * Creates a new instance of DBProject
    *
    * @param hostname the MySQL or PostgreSQL server hostname
    * @param database the name of the database
    * @param username the user name used to login to the database
    * @param password the user login password
    * @throws java.sql.SQLException when failed to make a connection.
    */
   public DBProject (String dbname, String dbport, String user, String passwd) throws SQLException {

      System.out.print("Connecting to database...");
      try{
         // constructs the connection URL
         String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
         System.out.println ("Connection URL: " + url + "\n");

         // obtain a physical connection
         // this._connection = DriverManager.getConnection("jdbc:postgresql://localhost:31629/postgres", user, passwd);
         this._connection = DriverManager.getConnection(url, user, passwd);

      }catch (Exception e){
         System.err.println("Error - Unable to Connect to Database: " + e.getMessage() );
         System.out.println("Make sure you started postgres on this machine");
         System.exit(-1);
      }//end catch
   }//end DBProject

   /**
    * Method to execute an update SQL statement.  Update SQL instructions
    * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
    *
    * @param sql the input SQL string
    * @throws java.sql.SQLException when update failed
    */
   public void executeUpdate (String sql) throws SQLException {
      // creates a statement object
      Statement stmt = this._connection.createStatement ();

      // issues the update instruction
      stmt.executeUpdate (sql);

      // close the instruction
      stmt.close ();
   }//end executeUpdate

   /**
    * Method to execute an input query SQL instruction (i.e. SELECT).  This
    * method issues the query to the DBMS and outputs the results to
    * standard out.
    *
    * @param query the input query string
    * @return the number of rows returned
    * @throws java.sql.SQLException when failed to execute the query
    */
   public int printQuery (String query) throws SQLException {
      // creates a statement object
      Statement stmt = this._connection.createStatement ();

      // issues the query instruction
      ResultSet rs = stmt.executeQuery (query);

      /*
       ** obtains the metadata object for the returned result set.  The metadata
       ** contains row and column info.
       */
      ResultSetMetaData rsmd = rs.getMetaData ();
      int numCol = rsmd.getColumnCount ();
      int rowCount = 0;

      // iterates through the result set and output them to standard out.
      boolean outputHeader = true;
      while (rs.next()){
	 if(outputHeader){
	    for(int i = 1; i <= numCol; i++){
		System.out.print(rsmd.getColumnName(i) + "\t");
	    }
	    System.out.println();
	    outputHeader = false;
	 }
         for (int i=1; i<=numCol; ++i)
            System.out.print (rs.getString (i) + "\t");
         System.out.println ();
         ++rowCount;
      }//end while
      stmt.close ();
      return rowCount;
   }//end executeQuery
   public List<List<String>> executeQuery(String query) throws SQLException {
      Statement stmt = this._connection.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      ResultSetMetaData rsmd = rs.getMetaData();
      int numCol = rsmd.getColumnCount();
      List<List<String>> result = new ArrayList<>();

      while (rs.next()) {
         List<String> row = new ArrayList<>();
         for (int i = 1; i <= numCol; i++) {
               row.add(rs.getString(i));
         }
         result.add(row);
      }
      stmt.close();
      return result;
   }
   /**
    * Method to close the physical connection if it is open.
    */
   public void cleanup(){
      try{
         if (this._connection != null){
            this._connection.close ();
         }//end if
      }catch (SQLException e){
         // ignored.
      }//end try
   }//end cleanup

   /**
    * The main execution method
    *
    * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
    */
   
   public static void main (String[] args) {
      if (args.length != 3) {
         System.err.println (
            "Usage: " +
            "java [-classpath <classpath>] " +
            DBProject.class.getName () +
            " <dbname> <port> <user>");
         return;
      }//end if
      
      Greeting();
      DBProject esql = null;
      try{
         // use postgres JDBC driver.
         Class.forName ("org.postgresql.Driver").newInstance ();
         // instantiate the DBProject object and creates a physical
         // connection.
         String dbname = args[0];
         String dbport = args[1];
         String user = args[2];
         esql = new DBProject (dbname, dbport, user, "");

         
         boolean keepon = true;
         while(keepon) {
            // These are sample SQL statements
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add new customer");
				System.out.println("2. Add new room");
				System.out.println("3. Add new maintenance company");
				System.out.println("4. Add new repair");
				System.out.println("5. Add new Booking"); 
				System.out.println("6. Assign house cleaning staff to a room");
				System.out.println("7. Raise a repair request");
				System.out.println("8. Get number of available rooms");
				System.out.println("9. Get number of booked rooms");
				System.out.println("10. Get hotel bookings for a week");
				System.out.println("11. Get top k rooms with highest price for a date range");
				System.out.println("12. Get top k highest booking price for a customer");
				System.out.println("13. Get customer total cost occurred for a give date range"); 
				System.out.println("14. List the repairs made by maintenance company");
				System.out.println("15. Get top k maintenance companies based on repair count");
				System.out.println("16. Get number of repairs occurred per year for a given hotel room");
				System.out.println("17. < EXIT");

            switch (readChoice()){
				   case 1: addCustomer(esql); break;
				   case 2: addRoom(esql); break;
				   case 3: addMaintenanceCompany(esql); break;
				   case 4: addRepair(esql); break;
				   case 5: bookRoom(esql); break;
				   case 6: assignHouseCleaningToRoom(esql); break;
				   case 7: repairRequest(esql); break;
				   case 8: numberOfAvailableRooms(esql); break;
				   case 9: numberOfBookedRooms(esql); break;
				   case 10: listHotelRoomBookingsForAWeek(esql); break;
				   case 11: topKHighestRoomPriceForADateRange(esql); break;
				   case 12: topKHighestPriceBookingsForACustomer(esql); break;
				   case 13: totalCostForCustomer(esql); break;
				   case 14: listRepairsMade(esql); break;
				   case 15: topKMaintenanceCompany(esql); break;
				   case 16: numberOfRepairsForEachRoomPerYear(esql); break;
				   case 17: keepon = false; break;
				   default : System.out.println("Unrecognized choice!"); break;
            }//end switch
         }//end while
      }catch(Exception e) {
         System.err.println (e.getMessage ());
      }finally{
         // make sure to cleanup the created table and close the connection.
         try{
            if(esql != null) {
               System.out.print("Disconnecting from database...");
               esql.cleanup ();
               System.out.println("Done\n\nBye !");
            }//end if
         }catch (Exception e) {
            // ignored.
         }//end try
      }//end try
   }//end main
   
   public static void Greeting(){
      System.out.println(
         "\n\n*******************************************************\n" +
         "              User Interface      	               \n" +
         "*******************************************************\n");
   }//end Greeting

   /*
    * Reads the users choice given from the keyboard
    * @int
    **/
   public static int readChoice()  {
      int input;
      // returns only if a correct value is given.
      do {
         System.out.print("Please make your choice: ");
         try { // read the integer, parse it and break.
            input = Integer.parseInt(in.readLine());
            break;
         }catch (Exception e) {
            System.out.println("Your input is invalid!");
            continue;
         }//end try
      }while (true);
      return input;
   }//end readChoice
   
   public static void addCustomer(DBProject esql) throws IOException, SQLException {
	  // Given customer details add the customer in the DB 
      // Your code goes here.
   
      int customerID; // NOT NULL
      String fName; // NOT NULL
      String lName; // NOT NULL
      
      //customerID, fname, lname CANNOT be Null

      String q = "SELECT MAX(customerID) FROM customer";
      List<List<String>> result = esql.executeQuery(q);
      String val = result.get(0).get(0);
      
      if(val == null || val.equals("null")) {
         customerID = 1;
      }
      else {
         customerID = Integer.parseInt(val) + 1;
      }
     
      
      do{
         System.out.print("Enter first name: ");
         fName = in.readLine();
         if(fName != null && !fName.trim().isEmpty()){ //to ensure that input is not empty
            break;
         } else {
            System.out.println("First name cannot be blank.");
         }
      } while(true);

      do{
         System.out.print("Enter last name: ");
         lName = in.readLine();
         if(lName != null && !lName.trim().isEmpty()){ //to ensure that input is not empty
            break;
         } else {
            System.out.println("Last name cannot be blank.");
         }
      } while(true);

      //all below can be Null
      System.out.print("Enter address: ");
      String Address = in.readLine();
      if(Address != null && Address.trim().isEmpty()){
         Address = null;
      }

      Long phNo = null;
      do {
         System.out.print("Enter phone number: ");
         String input = in.readLine();
         if(input == null || input.trim().isEmpty()){ //to ensure that input is not empty
            phNo = null;
            break;
         }
         if(input.matches("\\d{10}")){
            phNo = Long.parseLong(input);
            break;
         } else {
            System.out.println("Invalid phone number. Please enter exactly 10 digits.");
         }
      } while(true);

      
      String DOB = null;
      do{
         System.out.print("Enter date of birth (MM/DD/YYYY): ");
         String input = in.readLine();
         if (input == null || input.trim().isEmpty()) {
            DOB = null;
            break;
         } 
         if (!input.matches("^(0[1-9]|1[0-2])/(0[1-9]|[12]\\d|3[01])/\\d{4}$")) {
            System.out.println("Invalid format. Use MM/DD/YYYY.");
         } else {
            DOB = input;
            break;
         }
      } while(true);


      String gender = null;
      do {
         System.out.print("Enter gender (Male/Female/Other): ");
         String input = in.readLine();
         if (input == null || input.trim().isEmpty()) {
            gender = null;
            break;
         } 
         input = input.trim().toLowerCase();

         if (input.equals("male")) {
            gender = "Male";
            break;
         } else if (input.equals("female")) {
            gender = "Female";
            break;
         } else if (input.equals("other")) {
            gender = "Other";
            break;
         } else {
            System.out.println("Invalid gender. Enter Male, Female, or Other.");
         }
      } while (true);
      
      //Insert into database
      String addrValue;
      if (Address == null) {
         addrValue = "NULL";
      } else {
         addrValue = "'" + Address + "'";
      }

      String phValue;
      if (phNo == null) {
         phValue = "NULL";
      } else {
         phValue = phNo.toString();
      }

      String dobValue;
      if (DOB == null) {
         dobValue = "NULL";
      } else {
         dobValue = "'" + DOB + "'";
      }

      String genderValue;
      if (gender == null) {
         genderValue = "NULL";
      } else {
         genderValue = "'" + gender + "'";
      }

      // Insert into database
      String query =
         "INSERT INTO customer(customerID, fName, lName, Address, phNo, DOB, gender) VALUES (" +
         customerID + ", '" + fName + "', '" + lName + "', " +
         addrValue + ", " + phValue + ", " + dobValue + ", " + genderValue + ");";
   try {
      esql.executeUpdate(query);
      System.out.println("Customer added successfully!");
   } catch (Exception e) {
      System.err.println("Error adding customer: " + e.getMessage());
   }
   }//end addCustomer

   public static void addRoom(DBProject esql) throws IOException, SQLException {
	  // Given room details add the room in the DB
      // Your code goes here.
      //hotelID (integer) NOT NULL
      int hotelID;
      int roomNo;
      String roomType;
      
      
      //roomNo (integer) NOT NULL
      do{
         System.out.print("Enter Hotel number: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            try {
               hotelID = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid hotelID number. Try again.");
            }
         } else {
            System.out.println("Hotel ID cannot be blank.");
         }
      }while(true);

      String q = "SELECT MAX(roomNo) FROM room " + 
                 "WHERE hotelID = " + hotelID + ";";
      List<List<String>> result = esql.executeQuery(q);
      String val = result.get(0).get(0);
      
      if(val == null || val.equals("null")) {
         roomNo = 1;
      }
      else {
         roomNo = Integer.parseInt(val) + 1;
      }

      //roomType (String) NOT NULL
      do{
         System.out.print("Enter room type (default/Economy/Deluxe/Suite): ");
         roomType = in.readLine();
         if(roomType != null && !roomType.trim().isEmpty()){ //to ensure that input is not empty
            if(roomType.equalsIgnoreCase("default")|| roomType.equalsIgnoreCase("Economy")||roomType.equalsIgnoreCase("Deluxe")|| roomType.equalsIgnoreCase("Suite")){
               break;
            }
            System.out.println("Invalid room type. Enter default, Economy, Deluxe or Suite.");               
         } else {
            System.out.println("Room type cannot be blank.");
         }
      }while(true);    
      
      String query =
         "INSERT INTO room(hotelID, roomNo, RoomType) VALUES (" +
         hotelID + ", " + roomNo + ", '" + roomType + "');";
         
      try {
         esql.executeUpdate(query);
         System.out.println("Room added successfully!");
      } catch (Exception e) {
         System.err.println("Error adding Room: " + e.getMessage());
      }
   }

   public static void addMaintenanceCompany(DBProject esql) throws IOException, SQLException{
      // Given maintenance Company details add the maintenance company in the DB
      // cmpID Integer NOT NULL
      // name String NOT NULL
      // address String 
      // isCertified Bool NOT NULL
      int cmpID;
      String name;
      String address;
      boolean isCertified;

      
      String q = "SELECT MAX(cmpID) FROM maintenancecompany";
      List<List<String>> result = esql.executeQuery(q);
      String val = result.get(0).get(0);
      
      if(val == null || val.equals("null")) {
         cmpID = 1;
      }
      else {
         cmpID = Integer.parseInt(val) + 1;
      }
      

      do{
         System.out.print("Enter company name: ");
         name = in.readLine();
         if(name != null && !name.trim().isEmpty()){ //to ensure that input is not empty
            break;
         } else {
            System.out.println("Company name cannot be blank.");
         }
      } while(true);

      System.out.print("Enter Company address: ");
      address = in.readLine();
      if(address == null || address.trim().isEmpty()){
         address = null;
      }

      do{
         System.out.print("Is the company certified? (yes/no): ");
         String input = in.readLine();
         
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            input = input.trim().toLowerCase();
            if (input.equals("yes")) {
               isCertified = true;
               break;
            } else if (input.equals("no")) {
               isCertified = false;
               break;
            }
            System.out.println("Invalid input. Enter yes or no.");
         } else {
            System.out.println("Company certification cannot be blank.");
         }
      } while(true);
      String query;

      if (address == null) {
         query = "INSERT INTO maintenancecompany(cmpID, name, address, isCertified) VALUES (" +
                 cmpID + ", '" + name + "', " + "NULL" + ", " + isCertified + ");";
      } else {
         query = "INSERT INTO maintenancecompany(cmpID, name, address, isCertified) VALUES (" +
                 cmpID + ", '" + name + "', '" + address + "', " + isCertified + ");";
      }

      try {
         esql.executeUpdate(query);
         System.out.println("Maintenance company added successfully!");
      } catch (Exception e) {
         System.err.println("Error adding maintenance company: " + e.getMessage());
      }

   }//end addMaintenanceCompany

   public static void addRepair(DBProject esql) throws IOException, SQLException{
	  // Given repair details add repair in the DB
      // Your code goes here.
      int rID = 0;
      int hotelID = 0;
      int roomNo = 0;
      int mCompany = 0;
      String repairDate;
      String description = null;
      String repairType = null;

      
      String q = "SELECT MAX(rID) FROM repair";
      List<List<String>> result = esql.executeQuery(q);
      String val = result.get(0).get(0);
      
      if(val == null || val.equals("null")) {
         rID = 1;
      }
      else {
         rID = Integer.parseInt(val) + 1;
      }

      do{
         System.out.print("Enter the hotelID: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            try {
               hotelID = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid hotel ID. Try again.");
            }
         } else {
            System.out.println("hotelID cannot be blank.");
         }
      } while(true);

      do{
         System.out.print("Enter the Room number: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            try {
               roomNo = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid room number. Try again.");
            }
         } else {
            System.out.println("Room number cannot be blank.");
         }
      } while(true);

      do{
         System.out.print("Enter the Maintenance Company: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            try {
               mCompany = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid Maintenance Company. Try again.");
            }
         } else {
            System.out.println("Maintenance Company cannot be blank.");
         }
      } while(true);

      do{
         System.out.print("Enter date of repair (MM/DD/YYYY): ");
         String input = in.readLine();
         if (input == null || input.trim().isEmpty()) {
            repairDate = null;
            break;
         } 
         if (!input.matches("^(0[1-9]|1[0-2])/(0[1-9]|[12]\\d|3[01])/\\d{4}$")) {
            System.out.println("Invalid format. Use MM/DD/YYYY.");
         } else {
            repairDate = input;
            break;
         }
         
      } while(true);


      System.out.print("Enter the Description: ");
      String descInput = in.readLine();
      if(descInput != null && !descInput.trim().isEmpty()){ //to ensure that input is not empty
         description = descInput.replace("'", "''");
      }
      else {
         description = null;
      }
      
      do{
         System.out.print("Enter the Repair Type: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            if(input.length() > 10) {
               System.out.println("Repair type needs to be at most 10 char. Try again.");
               continue;
            }
            repairType = input;
            break;
         } else {
            repairType = null;
            break;
         }
      } while(true);

      String date;
      if(repairDate == null) {
         date = "NULL";
      }
      else {
         date = "'" + repairDate + "'";
      }

      String description_val;
      if(description == null) {
         description_val = "NULL";
      }
      else {
         description_val = "'" + description + "'";
      }

      String type;
      if(repairType == null) {
         type = "NULL";
      }
      else {
         type = "'" + repairType + "'";
      }



      String query = "INSERT INTO repair(rID, hotelID, roomNo, mCompany, repairDate, description, repairType) VALUES (" +
              rID + ", " + hotelID + ", " + roomNo + ", " + mCompany + ", " + date + ", " + description_val + ", " + type + ");";

      try {
         esql.executeUpdate(query);
         System.out.println("Repair added successfully!");
      } catch (Exception e) {
         System.err.println("Error adding repair: " + e.getMessage());
      }
   }//end addRepair

   public static void bookRoom(DBProject esql) throws IOException, SQLException{
	  // Given hotelID, roomNo and customer Name create a booking in the DB 
      // Your code goes here.
      // bID Numeric NOT NULL,
      // customer Numeric NOT NULL DEFAULT 0,
      // hotelID Numeric NOT NULL DEFAULT 0,
      // roomNo Numeric NOT NULL DEFAULT 0,
      // bookingDate Date NOT NULL,
      // noOfPeople Numeric,
      // price Numeric(6,2) NOT NULL

      int bID; //NOT NULL
      int customer; //NOT NULL
      int hotelID; //NOT NULL
      int roomNo; //NOT NULL
      String bookingDate; //NOT NULL
      Integer noOfPeople = null; //Can be NULL
      int price; //NOT NULL
     

      try {
         String q = "SELECT MAX(bID) FROM booking;";
         List<List<String>> result = esql.executeQuery(q);
         String maxIDstr = result.get(0).get(0);
         if(maxIDstr == null){
            bID = 1;
         } else {
            bID = Integer.parseInt(maxIDstr) + 1;
         }
      } catch (Exception e) {
         System.err.println("Error generating booking ID: " + e.getMessage());
         return;
      }
      
      do{
         System.out.print("Enter customer ID: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            try {
               customer = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid customer ID. Try again.");
            }
         } else {
            System.out.println("Company ID cannot be blank.");
         }
      }while(true);

      do{
         System.out.print("Enter hotel ID: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            try {
               hotelID = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid hotel ID. Try again.");
            }
         } else {
            System.out.println("Hotel ID cannot be blank.");
         }
      }while(true);
      
      do{
         System.out.print("Enter room ID: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            try {
               roomNo = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid room ID. Try again.");
            }
         } else {
            System.out.println("Room ID cannot be blank.");
         }
      }while(true);

      do{
         System.out.print("Enter booking date (MM/DD/YYYY): ");
         String input = in.readLine();
         if (input == null || input.trim().isEmpty()) {
            bookingDate = null;
            break;
         } 
         if (!input.matches("^(0[1-9]|1[0-2])/(0[1-9]|[12]\\d|3[01])/\\d{4}$")) {
            System.out.println("Invalid format. Use MM/DD/YYYY.");
         } else {
            bookingDate = input;
            break;
         }
      }while(true);
      

      do {
         System.out.print("Enter number of people: ");
         String input = in.readLine();
         if(input == null || input.trim().isEmpty()){ //to ensure that input is not empty
            noOfPeople = null;
            break;
         } else {
            try{
               noOfPeople = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid number of people. Try again.");
            }
         } 
      } while(true); //end bookRoom

      do{
         System.out.print("Enter room price: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            try {
               price = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid room price. Try again.");
            }
         } else {
            System.out.println("Price cannot be blank.");
         }
      }while(true);
      String query;
     
      query = "INSERT INTO booking(bID, customer, hotelID, roomNo, bookingDate, noOfPeople, price) VALUES (" +
               bID + ", " + customer + ", " + hotelID + ", " + roomNo + ", '" + bookingDate + "', " +
               (noOfPeople == null ? "NULL" : noOfPeople) + ", " + price + ");";
   
      try {
         esql.executeUpdate(query);
         System.out.println("Booking  added successfully!");
      } catch (Exception e) {
         System.err.println("Error adding booking: " + e.getMessage());
      }
   }
   
   public static void assignHouseCleaningToRoom(DBProject esql) throws IOException, SQLException {
	  // Given Staff SSN, HotelID, roomNo Assign the staff to the room 
      // Your code goes here.
      int asgID;
      int SSN;
      int hotelID;
      int roomNo;

      try {
         String q = "SELECT MAX(asgID) FROM assigned;";
         List<List<String>> result = esql.executeQuery(q);
         String val = result.get(0).get(0);
         if(val == null){
            asgID = 1;
         } else {
            asgID = Integer.parseInt(val) + 1;
         }
      } catch (Exception e) {
         System.err.println("Error generating assignment ID: " + e.getMessage());
         return;
      }
      
      do{
         System.out.print("Enter Staff SSN number: ");
         String input = in.readLine();
         if (input != null && !input.trim().isEmpty()) {
            try {
               SSN = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e) {
               System.out.println("Staff SSN number is invalid. Try Again.");
            }
         } else {
            System.out.println("SSN cannot be empty.");
         }

      }while(true);

      do{
         System.out.print("Enter Hotel ID: ");
         String input = in.readLine();
         if (input != null && !input.trim().isEmpty()) {
            try {
               hotelID = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e) {
               System.out.println("Hotel ID invalid. Try Again.");
            }
         } else {
            System.out.println("Hotel ID cannot be empty.");
         }

      }while(true);

      do{
         System.out.print("Enter room number: ");
         String input = in.readLine();
         if (input != null && !input.trim().isEmpty()) {
            try {
               roomNo = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e) {
               System.out.println("Room number is invalid. Try Again.");
            }
         } else {
            System.out.println("Room number cannot be empty.");
         }

      }while(true);

      String query = "INSERT INTO assigned(asgID, staffID, hotelID, roomNo) VALUES ("
                     + asgID + ", " + SSN + ", " + hotelID + ", " + roomNo + ");";
      
      try {
         esql.executeUpdate(query);
         System.out.println("Assignment added successfully!");
      } catch (Exception e) {
         System.err.println("Error adding Assignment: " + e.getMessage());
      }

      
   }//end assignHouseCleaningToRoom
   
   public static void repairRequest(DBProject esql) throws IOException, SQLException{ 
	  // Given a hotelID, Staff SSN, roomNo, repairID , date create a repair request in the DB
      // Your code goes here.
      // reqID Numeric NOT NULL,
      // managerID Numeric NOT NULL DEFAULT 0,
      // repairID Numeric NOT NULL DEFAULT 0,
      // requestDate Date NOT NULL,
      // description TEXT,
      
      int reqID; // Not NULL
      int managerID; // Not NULL
      int repairID; // Not NULL
      String requestDate;  // Not NULL
      String description = null; //Can be Null

      try {
         String q = "SELECT MAX(reqID) FROM request;";
         List<List<String>> result = esql.executeQuery(q);
         String val = result.get(0).get(0);
         if(val == null) {
            reqID = 1;
         } else {
            reqID = Integer.parseInt(val) + 1;
         }
      } catch (Exception e) {
         System.err.println("Error generating Request ID: " + e.getMessage());
         return;
      }

      do{
         System.out.print("Enter manager ID: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            try {
               managerID = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid manager ID. Try again.");
            }
         } else {
            System.out.println("Manager ID cannot be blank.");
         }
      }while(true);
      
      do{
         System.out.print("Enter repair ID: ");
         String input = in.readLine();
         if(input != null && !input.trim().isEmpty()){ //to ensure that input is not empty
            try {
               repairID = Integer.parseInt(input);
               break;
            } catch (NumberFormatException e){ //If it is not an integer
               System.out.println("Invalid repair ID. Try again.");
            }
         } else {
            System.out.println("Repair ID cannot be blank.");
         }
      }while(true);

      do{
         System.out.print("Enter request date (MM/DD/YYYY): ");
         String input = in.readLine();
         if (input != null && input.matches("^(0[1-9]|1[0-2])/(0[1-9]|[12]\\d|3[01])/\\d{4}$")) {
            requestDate = input;
            break;
         } else {
            System.out.println("Invalid format. Use MM/DD/YYYY.");
         }
      }while(true);
      
      System.out.print("Enter the Description: ");
      String descInput = in.readLine();
      if(descInput != null && !descInput.trim().isEmpty()){ //to ensure that input is not empty
         description = descInput.replace("'", "''");
      }
      else {
         description = null;
      }
      
       String query = "INSERT INTO request(reqID, managerID, repairID, requestDate, description) VALUES (" +
                     reqID + ", " + managerID + ", " + repairID + ", TO_DATE('" + requestDate + "', 'MM/DD/YYYY'), " +
                     (description == null ? "NULL" : "'" + description + "'") + ");";

   
      try {
         esql.executeUpdate(query);
         System.out.println("Repair Request added successfully!");
      } catch (Exception e) {
         System.err.println("Error adding repair request: " + e.getMessage());
      }

   }//end repairRequest
   
   public static void numberOfAvailableRooms(DBProject esql) throws IOException, SQLException{
	   // Given a hotelID, get the count of rooms available 
      // Your code goes here.
      try{
         System.out.print("\tEnter hotelID: ");
         String input = in.readLine();
         int hotelID = Integer.parseInt(input);
         String query = "SELECT COUNT(*) " +
                       "FROM room r " +
                       "WHERE r.hotelID = " + hotelID + " " +
                       "AND r.roomNo NOT IN (" +
                       "    SELECT b.roomNo " +
                       "    FROM booking b " +
                       "    WHERE b.hotelID = r.hotelID" +
                       ")";
         List<List<String>> result = esql.executeQuery(query);
         
         
         if (result.size() > 0 && result.get(0).size() > 0) {
            String count = result.get(0).get(0);
            System.out.println("Number of available rooms for hotel " + hotelID + " is " + count);
        } else {
            System.out.println("No available rooms found for hotel " + hotelID);
        }

      } catch(Exception e){
         System.err.println (e.getMessage());
      }
   }//end numberOfAvailableRooms
   
   public static void numberOfBookedRooms(DBProject esql) throws IOException, SQLException{
	  // Given a hotelID, get the count of rooms booked
      // Your code goes here.
      try{
         System.out.print("\tEnter hotelID: ");
         String input = in.readLine();
         int hotelID = Integer.parseInt(input);
         String query = "SELECT count(*) FROM booking WHERE hotelID = " + hotelID;
         List<List<String>> result = esql.executeQuery(query);
         if (result.size() > 0 && result.get(0).size() > 0) {
            String count = result.get(0).get(0);
            System.out.println ("Number of rooms booked for hotel " + hotelID + " is "  + count);
         } else {
            System.out.println("No bookings found for hotel " + hotelID);
         }
      } catch(Exception e){
         System.err.println (e.getMessage());
      }
   }//end numberOfBookedRooms
   
   public static void listHotelRoomBookingsForAWeek(DBProject esql) throws IOException, SQLException{
	  // Given a hotelID, date - list all the rooms booked for a week(including the input date) 
      // Your code goes here.
      try{
         System.out.print("\tEnter hotelID: ");
         String input = in.readLine();
         int hotelID = Integer.parseInt(input);
         System.out.print("\tEnter date in this format(MM/DD/YYYY): ");
         String date = in.readLine();

         String query = "SELECT roomNo, bookingDate FROM booking WHERE hotelID = " + hotelID + " AND bookingDate >= '" + date + "' AND bookingDate < (DATE '" + date + "' + 7)";
                        
         List<List<String>> result = esql.executeQuery(query);
         
         if (result.size() == 0) {
            System.out.println("No bookings for the given week at the hotel " + hotelID + ".");
        } 
        else {
         System.out.println("\nThe bookings, for hotel " + hotelID + " starting at " + date + ", for the week is:");
         for (List<String> row : result) {
            System.out.println("Room " + row.get(0) + " Booking Date: " + row.get(1));
         }
        }

      } catch(Exception e){
         System.err.println (e.getMessage());
      }
      
   }//end listHotelRoomBookingsForAWeek
   
   public static void topKHighestRoomPriceForADateRange(DBProject esql) throws IOException, SQLException{
	  // List Top K Rooms with the highest price for a given date range
      // Your code goes here.
      try{
         System.out.println("Enter start date (MM/DD/YYYY)");
         String startDate = in.readLine();
         System.out.println("Enter end date (MM/DD/YYYY)");
         String endDate = in.readLine();
         System.out.println("Enter K (number of top rooms with highest price)");
         int K = Integer.parseInt(in.readLine());

         String query = "SELECT r.hotelID, r.roomNo, b.price " +
                        "FROM room r " +
                        "JOIN booking b ON r.hotelID = b.hotelID AND r.roomNo = b.roomNo " +
                        "WHERE NOT (b.bookingDate < TO_DATE('" + startDate + "', 'MM/DD/YYYY') " +
                        "OR b.bookingDate > TO_DATE('" + endDate + "', 'MM/DD/YYYY')) " +
                        "ORDER BY b.price DESC " +
                        "LIMIT " + K;
                        
         List<List<String>> result = esql.executeQuery(query);
         
         if (result.size() > 0) {
            System.out.println ("Top " + K + " highest-priced available rooms from " + startDate + " to " + endDate + ":");
            for(int i = 0; i < result.size(); i++){
               List<String> row = result.get(i);
               System.out.println("hotelID: " + row.get(0) +
                                 ", RoomNo: " + row.get(1) +
                                 ", Price: " + row.get(2));
            }
         } else {
            System.out.println("No rooms available in the given date range");
         }
      
      } catch(Exception e){
         System.err.println (e.getMessage());
      }     
   }//end topKHighestRoomPriceForADateRange
   
   public static void topKHighestPriceBookingsForACustomer(DBProject esql) throws IOException, SQLException{
	  // Given a customer Name, List Top K highest booking price for a customer 
      // Your code goes here.
      try{
         System.out.print("\tEnter customer first name: ");
         String fname = in.readLine();
         System.out.print("\tEnter customer last name: ");
         String lname = in.readLine();
         System.out.println("Enter K (number of top rooms with highest price): ");
         int K = Integer.parseInt(in.readLine());

         String query = "SELECT B.bID, B.hotelID, B.roomNo, B.price " +
                        "FROM booking B, customer C " +
                        "WHERE B.customer = C.customerID " +
                        "AND C.fName = '" + fname + "' " +
                        "AND C.lName = '" + lname + "' " +
                        "ORDER BY B.price DESC " +
                        "LIMIT " + K + ";";

         List<List<String>> result = esql.executeQuery(query);

         if (result.size() == 0) {
            System.out.println("There are no bookings found for " + fname + " " + lname + ".");
         } else {
            System.out.println("\nThe " + K + " highest booking price for " + fname + " " + lname + ":");
            for (int i = 0; i < result.size(); ++i) {
               List<String> row = result.get(i);
               System.out.println("Booking ID: " + row.get(0) + ", Hotel ID: " + row.get(1) + ", Room Number: " + row.get(2) + ", Price: $" + row.get(3));
            }
         } 
            

      } catch (Exception e) {
         System.err.println("Error: " + e.getMessage());
      }
   }//end topKHighestPriceBookingsForACustomer
   
   public static void totalCostForCustomer(DBProject esql) throws IOException, SQLException{
	  // Given a hotelID, customer Name and date range get the total cost incurred by the customer
      // Your code goes here.
      try{
         System.out.print("\tEnter hotelID: ");
         String input = in.readLine();
         int hotelID = Integer.parseInt(input);
         System.out.print("\tEnter customer first name: ");
         String fname = in.readLine();
         System.out.print("\tEnter customer last name: ");
         String lname = in.readLine();
         System.out.print("\tEnter beggining of date range: ");
         String brange = in.readLine();
         System.out.print("\tEnter end of date range: ");
         String erange = in.readLine();


         String query = "SELECT SUM(B.price) " +
                        "FROM booking B, customer C " +
                        "WHERE B.customer = C.customerID " + "AND C.fName = '" + fname +
                        "' AND C.lName = '" + lname + "' " + "AND B.hotelID = " + hotelID + 
                        " AND B.bookingDate >= '" + brange + "' AND B.bookingDate <= '" + erange + "';";
                     
                        
         List<List<String>> result = esql.executeQuery(query);
         
         String totalCost = result.get(0).get(0);

         
         if (totalCost == null) {
            System.out.println("The customer had no cost for the week");
        } 
        else {
         System.out.println("\nThe total cost for the customer " + fname + " " + lname + " is $" + totalCost + " at hotel " + hotelID + 
         " with a date range of " + brange + " to " + erange);
        }

      } catch(Exception e){
         System.err.println (e.getMessage());
      }
      
   }//end totalCostForCustomer
   
   public static void listRepairsMade(DBProject esql) throws IOException, SQLException{
      // Given a Maintenance company name list all the repairs along with repairType, hotelID and roomNo
      // Your code goes here.
      try{
         System.out.println("Enter Maintenance company name: ");
         String companyName = in.readLine();
         String query = "SELECT r.repairType, r.hotelID, r.roomNo " +
                        "FROM repair r " +
                        "JOIN maintenancecompany m ON r.mCompany = m.cmpID " +
                        "WHERE m.name = '" + companyName + "' " +
                        "ORDER BY r.repairDate DESC";
         List<List<String>> result = esql.executeQuery(query);
        
         if (result.size() > 0) {
            System.out.println ("Repairs done by " + companyName + ":");
            for(int i = 0; i < result.size(); i++){
               List<String> row = result.get(i);
               System.out.println("RepairType: " + row.get(0).trim() + ", HotelID: " + row.get(1) + ", RoomNo: " + row.get(2));
            }
         } else {
            System.out.println("No repairs found for " + companyName);
         }
      } catch(Exception e){
         System.err.println (e.getMessage());
      }           
   }//end listRepairsMade
   
   public static void topKMaintenanceCompany(DBProject esql) throws IOException, SQLException{
	  // List Top K Maintenance Company Names based on total repair count (descending order)
      // Your code goes here.
      try {
         System.out.println("Enter K (number of top maintenance companys with highest total repair counts)");
         int K = Integer.parseInt(in.readLine());
      
         String query = "SELECT m.name, COUNT(r.rID) AS totalRepairs " + 
                        "FROM repair r " + 
                        "JOIN maintenancecompany m ON r.mCompany = m.cmpID " + 
                        "GROUP BY m.name " + 
                        "ORDER BY totalRepairs DESC " + 
                        "LIMIT " + K + ";";
         
         esql.printQuery(query);

      } catch (Exception e) {
         System.err.println(e.getMessage());
      }
     
   }//end topKMaintenanceCompany
   
   public static void numberOfRepairsForEachRoomPerYear(DBProject esql) throws IOException{
	  // Given a hotelID, roomNo, get the count of repairs per year
      // Your code goes here.
      try{
         System.out.println("Enter hotelID: ");
         int hotelID = Integer.parseInt(in.readLine());
         System.out.println("Enter room number: ");
         int roomNo  = Integer.parseInt(in.readLine());

         String query = "SELECT TO_CHAR(repairDate, 'YYYY') AS year, COUNT(*) AS repairCount " +
                        "FROM repair " +
                        "WHERE hotelID = " + hotelID + " " +
                        "AND roomNo = " + roomNo + " " +
                        "GROUP BY year " +
                        "ORDER BY year DESC;";
      
         esql.printQuery(query);

      } catch (Exception e) {
         System.err.println(e.getMessage());
      }
      
   }//end listRepairsMade

}//end DBProject
