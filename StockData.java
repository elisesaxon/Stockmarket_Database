/*******************************************
* 
* author: Elise Saxon - W01140640
* 
* WWU CS330 - Shameem Ahmed - Spring 2016
* May 20, 2016
* 
* Program queries a stock market database to get
* company stock info, then finds trading intervals, 
* then performs comparisons among companies in 
* each industry for each trading interval and 
* writes comparison data to a new database.
* 
********************************************/

import java.io.*;
import java.util.Properties;
import java.util.Scanner;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import static java.lang.Math.abs;


public class StockData {
   //create a global connection object to access database
   static Connection conn = null;
       
    /*
	   * Function: main
	   * parameters: command line arguments
	   * returns:    nothing
	   *
	   * The main function establishes a connection to the read database, initializes an industries list (to hold all data
	   * eventually), iterates through industries in list and calls functions to get data, perform calculations/comparisons, 
	   * and write new data to a write database. 
	   * 
	 */
   public static void main (String[] args) throws Exception {
      //load connection parameters file/get file name from command line or use default name
      String paramsFile = "readerparams.txt";
      if (args.length >= 1) {
         paramsFile = args[0];
      }
                
      //initialize Properties object to store connection properties
      Properties connectprops = new Properties();
      connectprops.load(new FileInputStream(paramsFile));
                
      try {
         //get connection properties from text file, establish connection
         Class.forName("com.mysql.jdbc.Driver");
         String dburl = connectprops.getProperty("dburl");
         String username = connectprops.getProperty("user");
         conn = DriverManager.getConnection(dburl, connectprops);
         System.out.printf("Database connection %s %s established.%n", dburl, username);
      } 
      catch (SQLException ex) {
         System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
      }
                
                
      //initialize list of Industries objects
      ArrayList<Industries> industries = new ArrayList<Industries> ();
                
      //call function to get a list of all industries in database
      getIndustries(industries);
      
      //iterating through all the industries
      for (int i = 0; i < industries.size(); i++) {
         System.out.println(i +": Processing " + industries.get(i).name);
                        
         //call function to get companies and date info for each industry
         getCompanies(industries, i);
                        
         //call function to fill a list of companies in each industry
         getCompanyData(industries, i);
                        
         //call function to adjust open and close prices for splits
         computeSplits(industries, i);
         
         //call function to get intervals and data
         findIntervals(industries, i);
            
         //call function to calculate and save ticker return
         tickerReturn(industries, i);
         
         //call function to compare companies and save industry return
         industryReturn(industries, i);
      }
      
      //create table once, then fill industry-by-industry
      createDbTable(args);
     
      //iterate through industries, write to saxone DB table
      for(int m = 0; m < industries.size(); m++) {
         //call function to write data to saxone database
         writeToDatabase(industries, m);
      }
                
   }
       
       
    /*
	   * Function: getIndustries
	   * parameters: empty industries ArrayList
	   * returns:    nothing
	   *
	   * Queries the read database to get all the stock industries found in the database, 
	   * create an Industries object for each one, and add that object to industries list.
	 */   
   public static void getIndustries (ArrayList<Industries> industries) throws SQLException {
      PreparedStatement getInd = conn.prepareStatement("select distinct Industry from Company");
                             
      //execute query, get results
      ResultSet results = getInd.executeQuery();
      while (results.next()) {
         //create new Industries object, add to list of industries
         Industries ind = new Industries(results.getString("Industry"));
         industries.add(ind);
      } 
      getInd.close();
   }
       
   
    /*
	   * Function: getCompanies
	   * parameters: industries arrayList, current industry index
	   * returns:    nothing
	   *
	   * Queries read database to get Tickers, minimum and maximum trading dates, and number of
	   * trading days for each company in an industry. Compares minimum date of each company 
	   * against each other to find the highest minimum date (similarly with the maximum date 
	   * of each company to find the lowest maximum date) in order to create a date range for 
	   * the industry. Uses those values to create a Company object and adds that to a list 
	   * within the Industries object of companies in the industry.
	 */  
   public static void getCompanies (ArrayList<Industries> industries, int i) throws SQLException {
      //variables to hold initial begin/end dates from query
      String ticker = "";
      String minDate = "";
      String maxDate = "";
      int mindays = 0;
              
      //variables to hold final comparison values
      String minD = "";
      String maxD = "";
      int days = 0;
              
      PreparedStatement getComp = conn.prepareStatement("select Ticker, min(TransDate) as minDay, max(TransDate) as maxDay, count(distinct TransDate) as numDays " + 
                                                                 " from Company natural left outer join PriceVolume " +
                                                                 " where Industry = ? " +
                                                                 " group by Ticker " +
                                                                 " having numDays >= 150 " +
                                                                 " order by Ticker");
      getComp.setString(1, industries.get(i).name);
              
      ResultSet results = getComp.executeQuery();
      while (results.next()) {
         //get values from query, store as variables
         ticker = results.getString("Ticker");
         minDate = results.getString("minDay");
         maxDate = results.getString("maxDay");
         days = results.getInt("numDays");
         
         //compare max/min dates of each company against each other to find the lowest max and highest min date
         if ((minD == "") || (minD.compareTo(minDate) < 0)) {
            //current min is less than this minDate (new maxmin)
            minD = minDate;
         }
         if ((maxD == "") || (maxD.compareTo(maxDate) > 0)) {
            //current max is greater than this maxDate (new minmax)
            maxD = maxDate;
         }
         
         //get accurate number of days
         if (mindays > days) {
            mindays = days;
         }
         
         //create Company object with final min/max/days values
         Company comp = new Company(ticker, minD, maxD, days);
         industries.get(i).addCompany(comp);
                     
      }
      getComp.close();
              
      days = mindays;
      for (int j = 0; j < industries.get(i).companies.size(); j++) {
         //set final minimum/maximum dates and number of trading days for Company objects in this industry
         industries.get(i).companies.get(j).setMin(minD);
         industries.get(i).companies.get(j).setMax(maxD);
         industries.get(i).companies.get(j).setDays(mindays);          
      }    
   }

       
    /*
	   * Function: getCompanyData
	   * parameters: industries arraylist, current industry index
	   * returns:    nothing
	   *
	   * Queries read database to get PriceVolume data (open/close prices and date) for each 
	   * company for each date within the industry's date range. Saves that data in a 
	   * CompanyData object and adds it to a list of CompanyData objects within each Company object
	   * within each Industry object.
	 */  
   public static void getCompanyData (ArrayList<Industries> industries, int i) throws SQLException {
      //query to get PriceVolume data for each company for each day within date range        
      PreparedStatement getData = conn.prepareStatement("select Ticker, P.TransDate, P.openPrice, P.closePrice " +
                                                                     " from PriceVolume P natural join Company " +
                                                                     " where Industry = ? and TransDate between ? and ? " + 
                                                                     " order by Ticker, TransDate");
      getData.setString(1, industries.get(i).name);
      getData.setString(2, industries.get(i).companies.get(1).minDate);
      getData.setString(3, industries.get(i).companies.get(1).maxDate);
              
              
      ResultSet results = getData.executeQuery();
      while (results.next()) {
         //save query info in variables
         String tkr = results.getString(1);
         String trans = results.getString(2);
         double open = results.getDouble(3);
         double close = results.getDouble(4);
         
         //create new CompanyData object for this company, for this day
         CompanyData comp = new CompanyData(tkr, trans, open, close);
                     
         //iterate through companies in this industry
         for(int k = 0; k < industries.get(i).companies.size(); k++) {
            //ticker from query matches current company's ticker
            if (tkr.equals(industries.get(i).companies.get(k).ticker)) {
               //add CompanyData object to list of days/info for this company
               industries.get(i).companies.get(k).addComp(comp);
            } 
         }  
         
      }
      getData.close();    
   }
       
       
    /*
	   * Function: computeSplits
	   * parameters: industries arraylist, current industry index
	   * returns:    nothing
	   *
	   * Goes through every day of data for each company in an industry and 
	   * adjusts the open/close prices of each day based on splits found.
	 */   
   public static void computeSplits (ArrayList<Industries> industries, int i) {
      double divisor = 1;
               
      //iterate through companies (j) in industry, days (k)
      for (int j = 0; j < industries.get(i).companies.size()-1; j++) {
         for (int k = 0; k < industries.get(i).companies.get(j).companyDays.size()-1; k++) {
            //set day variables for day and nextday
            CompanyData day = industries.get(i).companies.get(j).companyDays.get(k);
            CompanyData nextDay = industries.get(i).companies.get(j).companyDays.get(k+1);
                        
            //adjust prices for previous splits
            if (divisor > 0) {
               day.adjustPrice((day.open)/divisor, (day.close)/divisor);
            }
                        
            //calculate split determiners
            double twoOne = day.close / nextDay.open - 2.0;
            double threeOne = day.close / nextDay.open - 3.0;
            double threeTwo = day.close / nextDay.open - 1.5;
                        
            // 2:1 split
            if (abs(twoOne) < 0.20) {     
               //adjust today's prices based on split
               day.adjustPrice((day.open)/2, (day.close)/2);
                                
               //uptdate total divisor for all previous days
               divisor = divisor*2;
            } 
            // 3:1 split
            if (abs(threeOne) < 0.30) {
               day.adjustPrice((day.open)/3, (day.close)/3);
               divisor = divisor*3;
            } 
            // 3:2 split
            if (abs(threeTwo) < 0.15) {
               day.adjustPrice((day.open)/1.5, (day.close)/1.5);
               divisor = divisor*1.5;
            } 
         }
      }
   }
       
       
    /*
	   * Function: findIntervals
	   * parameters: industries arraylist, current industry index
	   * returns:    nothing
	   *
	   * Goes through data for each day of each company in each industry to set 
	   * 60-day interval dates for future comparison. Starts with the first (alphabetical)
	   * company in the industry and sets the interval start and end dates based on that 
	   * company's transaction dates. Then iterates through the rest of he companies and 
	   * sets their start/end dates based on the first company's. For each interval there 
	   * is an Interval object containing the number of the interval, a list of 
	   * CompanyData objects of start date data, and a list of CompanyData objects of end
	   * date data. Each of these Interval objects is stored in a list of intervals within 
	   * the Industries object for each industry.
	 */  
   public static void findIntervals (ArrayList<Industries> industries, int i) {
      /* SET THE INTERVAL DATES FOR THE FIRST (alphabetical) COMPANY: */
      int intervalCount = 1;
              
      //create first interval object 
      Interval intvl = new Interval(intervalCount);
      
      if ((industries.get(i).companies.size() > 0) && (industries.get(i).companies.get(0).companyDays.size() > 0)) {       
         //add first day of first company to starting days list of first interval, add interval object to intervals list
         intvl.addStartDate(industries.get(i).companies.get(0).companyDays.get(0));
         industries.get(i).addInterval(intvl);
      }
                     
      //iterate through trading days for first company
      for (int k = 1; k < industries.get(i).companies.get(0).companyDays.size(); k++) {
         if (k%60 == 0) {
            //interval end day--add CompanyData object to endDates list for current interval
            industries.get(i).intervals.get(intervalCount-1).addEndDate(industries.get(i).companies.get(0).companyDays.get(k-1));
                                    
            //new interval
            intervalCount++;
                                    
            //make new Interval object for next interval, add first company to start dates list, add interval object to list
            Interval nextInterval = new Interval(intervalCount);
            nextInterval.addStartDate(industries.get(i).companies.get(0).companyDays.get(k));
            industries.get(i).intervals.add(nextInterval);
         }
      }
              
              
      /* SET INTERVAL DATES FOR THE REST OF THE COMAPNIES BASED ON THE FIRST COMPANY'S DATES: */
      //iterate through rest of companies in industry
      for (int j = 1; j < industries.get(i).companies.size(); j++) {
                     
         //iterate through intervals
         for (int n = 0; n < industries.get(i).intervals.size()-1; n++) {
            //get predetermined starting and ending dates for interval and next interval's start date
            String intvlStart = industries.get(i).intervals.get(n).startDates.get(0).trans;
            String intvlEnd = industries.get(i).intervals.get(n).endDates.get(0).trans;
            String nextIntStart = "";
            if(n+1 != industries.get(i).intervals.size()){
               nextIntStart = industries.get(i).intervals.get(n+1).startDates.get(0).trans;
            }
                              
            //iterate through days of data for companies, add companies to start/end date lists of interval based on dates rules
            for (int m = 0; m < industries.get(i).companies.get(j).companyDays.size(); m++) {
                              
               //current comp's trans date = determined intvl start, add this day to start dates for intvl
               if (industries.get(i).companies.get(j).companyDays.get(m).trans.equals(intvlStart)) {
                  industries.get(i).intervals.get(n).addStartDate(industries.get(i).companies.get(j).companyDays.get(m));
                                    
               //if current comp's date < intvlStart AND next date > intvlStart, add next date to start
               } else if ((industries.get(i).companies.get(j).companyDays.get(m).trans.compareTo(intvlStart) < 0) && (industries.get(i).companies.get(j).companyDays.get(m+1).trans.compareTo(intvlStart) > 0)) {
                  industries.get(i).intervals.get(n).addStartDate(industries.get(i).companies.get(j).companyDays.get(m+1));
                                    
               //not a start date; do nothing
               } else {
                  n=n;
               }
                                    
               //NOT the last interval AND if current comp's trans date = next interval start, add PREVIOUS comp's day to this interval's end dates
               if ((n != industries.get(i).intervals.size()-1) && (industries.get(i).companies.get(j).companyDays.get(m).trans.equals(nextIntStart))) {
                  industries.get(i).intervals.get(n).addEndDate(industries.get(i).companies.get(j).companyDays.get(m-1));
                                    
               //last interval AND current comp's trans date is on or before end date of last interval
               } else if ((n == industries.get(i).intervals.size()-1) && (industries.get(i).companies.get(j).companyDays.get(m).trans.compareTo(intvlEnd) <= 0))  {
                  industries.get(i).intervals.get(n).addEndDate(industries.get(i).companies.get(j).companyDays.get(m));
                                    
               //not an end date; do nothing
               } else {
                  n=n;
               }
            }
         }
      }
   }
       
       
    /*
	   * Function: tickerReturn
	   * parameters: industries arraylist, current industry index
	   * returns:    nothing
	   *
	   * Iterates through each interval's start/end dates, calculates the ticker return
	   * value, which is a day's (close price / open price) - 1. Saves that value in 
	   * the TickerReturn field of the CompanyData object in the StartDates list of the
	   * interval object.
	 */ 
   public static void tickerReturn (ArrayList<Industries> industries, int i) {
      double tkrReturn = 0;
      
      //iterate through trading intervals
      for (int j = 0; j < industries.get(i).intervals.size()-1; j++) {
         //iterate through company start/end dates for this interval
         for (int k = 0; k < industries.get(i).intervals.get(j).startDates.size(); k++) {
            //ticker return = (day.close / day.open) - 1
            tkrReturn = (industries.get(i).intervals.get(j).endDates.get(k).close / industries.get(i).intervals.get(j).startDates.get(k).open) - 1;
            
            //add ticker return value to field of CompanyData object for this day/company. ***TickerReturn and IndustryReturn stored in CompanyData object of START DATES LIST for an interval***
            industries.get(i).intervals.get(j).startDates.get(k).addTickerReturn(tkrReturn);
         }
      }
   }    
         
       
    /*
	   * Function: industryReturn
	   * parameters: industries arraylist, current industry index
	   * returns:    nothing
	   *
	   * Calculates each company's IndustryReturn by comparing the company's 
	   * TickerReturn to the rest of the industry's companies' ticker returns. 
	   * Iterates through each interval's startDates to add up each company's TickerReturn
	   * values (calculated in the function directly above). Subtracts the current 
	   * company's TickerReturn value, divides that by one less than the number of 
	   * companies in the industry. This gives us the IndustryReturn to save in the 
	   * IndustryReturn field of the CompanyData object in the StartDates list of the
	   *  interval object.
	 */ 
   public static void industryReturn (ArrayList<Industries> industries, int i) {
      //iterate through intervals
      for (int j = 0; j < industries.get(i).intervals.size(); j++) {
         
         //iterate through CompanyData objects in interval 
         for (int k = 0; k < industries.get(i).intervals.get(j).startDates.size(); k++) {
            //initialize money variable at each iteration
            double money = 0;
            
            //add up all (previously calculated) company TickerReturn values
            for (int m = 0; m < industries.get(i).intervals.get(j).startDates.size(); m++) {
               money += industries.get(i).intervals.get(j).startDates.get(m).TickerReturn;
            }
            
            //variable to hold value of (1 / (number of companies - 1))
            double numComps = industries.get(i).companies.size()-1;
            numComps = 1/numComps;
         
            //subtract this current company's TickerReturn, multiply by 1/(number of companies - 1) 
            money -= industries.get(i).intervals.get(j).startDates.get(k).TickerReturn;
            money = money * numComps;
            
            //save IndustryReturn for this company. ***TickerReturn and IndustryReturn stored in CompanyData object of START DATES LIST for an interval***
            industries.get(i).intervals.get(j).startDates.get(k).addIndustryReturn(money);
         }
      }
   }
   
   
    /*
	   * Function: createDbTable
	   * parameters: commandline arguments
	   * returns:    nothing
	   *
	   * Establishes a connection with the write database, deletes a possible 
	   * pre-existing Performance table in the database, and creates a new empty
	   * Performance table to write data to.
	 */ 
   public static void createDbTable (String [] args) throws Exception, SQLException {
      //load connection parameters file/get file name from command line or use default name
      String paramsFile = "writerparams.txt";
      if (args.length >= 1) {
         paramsFile = args[1];
      }
                
      //initialize Properties object to store connection properties
      Properties connectprops = new Properties();
      connectprops.load(new FileInputStream(paramsFile));
                
      try {
         //get connection properties from text file, establish connection
         Class.forName("com.mysql.jdbc.Driver");
         String dburl = connectprops.getProperty("dburl");
         String username = connectprops.getProperty("user");
         conn = DriverManager.getConnection(dburl, connectprops);
         System.out.printf("Database connection %s %s established.%n", dburl, username);
      } 
      catch (SQLException ex) {
         System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
      }
      
      
      //drop Performance table each time program runs
      PreparedStatement dropTable = conn.prepareStatement("drop table if exists Performance");
      dropTable.close();
      
      //create Performance table in saxone database
      PreparedStatement createTable = conn.prepareStatement("create table Performance " +
                                                             " (Industry CHAR(30), " +
                                                             "  Ticker CHAR(6), " +
                                                             "  StartDate CHAR(10), " +
                                                             "  EndDate CHAR(10), " +
                                                             "  TickerReturn CHAR(12), " +
                                                             "  IndustryReturn CHAR(12))");
      createTable.executeUpdate();
      
      createTable.close();
   }
   
   
    /*
	   * Function: writeToDatabase
	   * parameters: industries arraylist, current industry index
	   * returns:    nothing
	   *
	   * Writes industry, company, comparison data to the Performance table
	   * in the write database. For an industry, iterates through each interval
	   * and StartDates list, writes an sql command to add a tuple to the 
	   * table, and fills in the values of the command with the industry name, 
	   * ticker (company), interval start date, interval end date, company's 
	   * ticker return, and company's industry return.
	 */ 
   public static void writeToDatabase (ArrayList<Industries> industries, int i) throws SQLException {
      //iterate through intervals
      for (int j = 0; j < industries.get(i).intervals.size(); j++) {
         //iterate through CompanyData objects in startDates list for this interval
         for (int k = 0; k < industries.get(i).intervals.get(j).startDates.size(); k++) {
            
            //write data to table if it exists 
            if ((industries.get(i).intervals.size() > 0) && (industries.get(i).intervals.get(j).startDates.size() > 0) && (industries.get(i).intervals.get(j).endDates.size() > 0)) {
               PreparedStatement addTuple = conn.prepareStatement("insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn) " +
                                                                " values (?, ?, ?, ?, ?, ?)");
            
               //fill in values for query from data stored in Intervals objects
               addTuple.setString(1, industries.get(i).name);
               addTuple.setString(2, industries.get(i).intervals.get(j).startDates.get(k).ticker);
               addTuple.setString(3, industries.get(i).intervals.get(j).startDates.get(0).trans);
               addTuple.setString(4, industries.get(i).intervals.get(j).endDates.get(0).trans);
               addTuple.setString(5, String.format("%10.7f", industries.get(i).intervals.get(j).startDates.get(k).TickerReturn));
               addTuple.setString(6, String.format("%10.7f", industries.get(i).intervals.get(j).startDates.get(k).IndustryReturn));
               
               addTuple.executeUpdate();
               
               addTuple.close();
            }
         }
      }
   }
   
}