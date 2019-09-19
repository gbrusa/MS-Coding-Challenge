import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * <b>Main</b> reads data from a given CSV file and inserts valid records into a SQLite database.
 * Invalid records will be written to an output file.
 * 
 * @author Leo Brusa
 *
 */
public class Main {
	static final int NUM_HEADERS = 10;
	static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS tbl(\n"
			+ "    A text, \n"
			+ "    B text, \n"
			+ "    C text, \n"
			+ "    D text, \n"
			+ "    E text, \n"
			+ "    F text, \n"
			+ "    G text, \n"
			+ "    H text, \n"
			+ "    I text, \n"
			+ "    J text \n"
			+ ");";
	static final String INSERT_INTO = "INSERT INTO tbl(A, B, C, D, E, F, G, H, I, J) VALUES(?,?,?,?,?,?,?,?,?,?);";
	
	// variables that store processing statistics
    private static int success = 0;
    private static int failure = 0;
    private static int total = 0;
    
    public static void main(String[] args) {
        if (args.length < 1) {
        	usage();
        }
        
        // extract the fileName from the given argument
        String fileName = args[0].substring(0, args[0].length()-4);
    	
        // create url for connecting to sqlite database
    	String url = "jdbc:sqlite:" + fileName + ".db";

    	// create bad output file if it doesn't already exist
    	File badOutput = new File(fileName + "-bad.csv");
    	try {
    		if (!badOutput.exists()) badOutput.createNewFile();
    	} catch (IOException e) {
    		System.err.println("[ERROR] Failed to create " + fileName + "-bad.csv");
    		System.exit(1);
    	}
    	
        try (Connection conn = DriverManager.getConnection(url);
        		PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(badOutput, true)))) {
            if (conn != null) {
                // established connection with database
            	System.out.println("New database, " + fileName + ".db, successfully created.");
                
            	// create table
                createTable(conn);
                System.out.println("Table, tbl(A, B, C, D, E, F, G, H, I, J), successfully created.");
                
                // begin processing data records
                System.out.println("Processing data:");
                processData(conn, fileName + ".csv", pw);
                
            }
        } catch (SQLException e) {
            System.err.println("[ERROR] Failed to create new database, " + fileName + ".db");
            System.exit(1);
        } catch (IOException e2) {
			System.err.println("[ERROR] Filed to write to, " + fileName + "-bad.csv");
			System.exit(1);
		}
        
        // print processing log counts
    	printLogs(fileName + ".log");
    	
    	// we are all done!
    	System.out.println("Finished processing.");
    }
    
    /**
     * Creates a SQLite table as per the constant statement, CREATE_TABLE.
     * @param conn
     * 			The connection to the SQLite database
     */
    private static void createTable(Connection conn) {
    	try (Statement s = conn.createStatement()) {
    		s.execute(CREATE_TABLE);
    	} catch (SQLException e) {
    		System.err.println("[ERROR] Unable to create table.");
    		System.exit(1);
    	}
    }
    
    /**
     * Reads and processes the data stored in fileName.csv.
     * Data rows that have 10 values to insert into the table will be inserted. Otherwise, they
     * will be written to an output .csv file.
     * @param conn
     * 			The connection to the SQLite database
     * @param fileName
     * 			The name of the file from which to read
     * @param pw
     * 			The writer that points to the bad output csv file
     */
    private static void processData(Connection conn, String fileName, PrintWriter pw) {
        Consumer<String> action = x -> {
        	// regex only splits commas that are not escaped by double quotes
        	String[] values = x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        	
        	if (!x.contains(",,") && values.length == NUM_HEADERS) {
        		// if record contains double comma, we know we are missing data
        		// if there are more or less values than the number of headers, then we
        		// know the record is invalid
        		insertInto(conn, values);
        		success++;
        	} else {
        		// else we append to writer, which will flush at the end when it's closed
        		pw.append(x + "\n");
        		failure++;
        	}
        	
        	total++;
        	// prints updates every thousand records
        	if (total % 1000 == 0) {
        		System.out.println("\t" + total + "...");
        	}
        };
        
        // we read the file and perform the lambda expression on each record
    	try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
    		stream.forEach(action);
    	} catch (IOException e) {
    		System.err.println("[ERROR] Failed to read from file, " + fileName);
    		System.exit(1);
    	}
    }
    
    /**
     * Inserts the given values into the database given by the connection by
     * using the prepared statement, INSERT_INTO.
     * @param conn
     * 			The connection to the SQLite database
     * @param values
     * 			The values to insert into the prepared statement, in the given order
     */
    private static void insertInto(Connection conn, String[] values) {
    	try (PreparedStatement ps = conn.prepareStatement(INSERT_INTO)) {
    		for (int i = 0; i < values.length; i++) {
    			ps.setString(i+1, values[i]);
    		}
    		ps.execute();
    	} catch (SQLException e) {
			System.err.println("[ERROR] Unable to insert into table.");
			System.exit(1);
		}
    }
    
    /**
     * Prints the received, success, and failure logs to a .log file.
     * @param fileName
     * 			The name of the log file
     */
    private static void printLogs(String fileName) {
    	File logOutput = new File(fileName);
    	try {
    		logOutput.createNewFile();
    	} catch (IOException e) {
    		System.err.println("[ERROR] Unable to create " + fileName);
    		return;
    	}
    	
    	try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(logOutput, true)))) {
    		pw.println("Received   : " + total + "\n" +
    				   "Successful : " + success + "\n" +
    				   "Failed     : " + failure);
    	} catch (IOException e) {
			System.err.println("[ERROR] Failed to print to log file.");
		}
    }
    
    /**
     * Prints out the application usage and exits.
     */
    private static void usage() {
    	System.err.println("Required arguments: filename");
    	System.exit(1);
    }
}