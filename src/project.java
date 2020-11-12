import java.sql.*;
import java.io.*;


public class project {
	
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public project(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	 public void executeUpdate(String sql) throws SQLException {
	        // creates a statement object
	        Statement stmt = this._connection.createStatement();
	 
	        // issues the update instruction
	        stmt.executeUpdate(sql);
	 
	        // close the instruction
	        stmt.close();
	    }// end executeUpdate
	 
	 public int executeQuery(String query) throws SQLException {
	        // creates a statement object
	        Statement stmt = this._connection.createStatement();
	 
	        // issues the query instruction
	        ResultSet rs = stmt.executeQuery(query);
	 
	        /*
	         ** obtains the metadata object for the returned result set. The metadata
	         ** contains row and column info.
	         */
	        ResultSetMetaData rsmd = rs.getMetaData();
	        int numCol = rsmd.getColumnCount();
	        int rowCount = 0;
	 
	        // iterates through the result set and output them to standard out.
	        boolean outputHeader = true;
	        while (rs.next()) {
	            if (outputHeader) {
	                for (int i = 1; i <= numCol; i++) {
	                    System.out.print(rsmd.getColumnName(i) + "\t");
	                }
	                System.out.println();
	                outputHeader = false;
	            }
	            for (int i = 1; i <= numCol; ++i)
	                System.out.print(rs.getString(i) + "\t");
	            System.out.println();
	            ++rowCount;
	        } // end while
	        stmt.close();
	        return rowCount;
	    }// end executeQuery
	 
	 
	 public void cleanup() {
	        try {
	            if (this._connection != null) {
	                this._connection.close();
	            } // end if
	        } catch (SQLException e) {
	            // ignored.
	        } // end try
	    }// end cleanup
	 
	 
	public static void main (String[] args) throws Exception {
		
		
		
		project esql = null;
		
		try {
		
			Class.forName("org.postgresql.Driver");
			
			
			String dbname = "pjfall2020";
			String dbport = "5432";
			String uname = "postgres";
			String password = "123581321";
			
		
			
			esql = new project(dbname, dbport, uname, password);
			boolean running = true;
			
			while(running) {
	
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("0. Test (Just a test should print 'Dylan'");
				System.out.println("1. View Video ");
				System.out.println("2. Upload a video");
				System.out.println("3. Delete a video");
				System.out.println("4. Search for a video");
				System.out.println("5. View recommended videos"); 
				System.out.println("6. View most popular videos");
				System.out.println("7. View most popular channels");
				System.out.println("11. EXIT");
				
				 switch (readChoice()) {
				   case 0: testcase(esql); break;
				   case 1: viewVideo(esql); break;
				   case 2: uploadVideo(esql); break;
				   case 3: deleteVideo(esql); break;
				   case 4: searchVideo(esql); break;
				   case 5: viewRecommendedVideos(esql); break;
				   case 6: popVideos(esql); break;
				   case 7: popchannels(esql); break;
				   case 11: running = false; break;
				   default : System.out.println("Unrecognized choice!"); break;
				
				 } // end switch
			
			} // end while
			
		} catch (Exception e) {
				System.err.println(e.getMessage());
		} finally {
			// make sure to cleanup the created table and close the connection.
	        try {
	        	if (esql != null) {
	        		System.out.print("Disconnecting from database...");
	                esql.cleanup();
	                System.out.println("Done\n\nBye !");
	            } // end if
	        } catch (Exception e) {
                // ignored.
	        }
		} // end finally
	} //end main
	
	
	// from old project
	public static int readChoice() {
		
		int input;
		// returns only if a correct value is given.
		do {
			
			System.out.print("Please make your choice: ");
			
			try { // read the integer, parse it and break
				
				input = Integer.parseInt(in.readLine());
		        break;
		        
		    } catch (Exception e) {
		    	System.out.println("Your input is invalid!");
		        continue;
		    }//end try
		} 
		while (true);
		      
		return input;
		 
	} //end readChoice
	
	public static void testcase(project esql) {
		
		try {
        	String query = "select fname from user1 where uid = 3;";
        	
            esql.executeQuery(query);
 
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
		
	}
	public static void viewVideo(project esql) {
	     try {
	            String query = 	"";
	            
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	public static void uploadVideo(project esql) {
	     try {
	            String query = 	"";
	            
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	public static void deleteVideo(project esql) {
	     try {
	            String query = 	"";
	            
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	public static void searchVideo(project esql) {
	     try {
	            String query = 	"";
	            
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	public static void viewRecommendedVideos(project esql) {
	     try {
	            String query = 	"";
	            
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	public static void popVideos(project esql) {
	     try {
	            String query = 	"";
	            
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	public static void popchannels(project esql) {
	     try {
	            String query = 	"";
	            
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	
	
}
