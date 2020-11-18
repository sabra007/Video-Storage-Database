import java.sql.*;
import java.util.Calendar;
import java.io.*;


public class project {
	
	private static Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	static String username = "";
	static int userid;
	static boolean loggedin = false;
	
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
			
			username = "Not logged in";
			
			
			esql = new project(dbname, dbport, uname, password);
			boolean running = true;
			
			while(running) {
	
				System.out.println("\nMAIN MENU");
				System.out.println("Logged in as: " + username);
				System.out.println("---------");
				if(!loggedin)
					System.out.println("0. Login");
				else
					System.out.println("0. Logout");
				System.out.println("1. View Video ");
				System.out.println("2. Upload a video");
				System.out.println("3. Delete a video");
				System.out.println("4. Search for a video");
				System.out.println("5. View recommended videos"); 
				System.out.println("6. View most popular videos");
				System.out.println("7. View most popular channels");
				System.out.println("8. Create a channel");
				System.out.println("11. EXIT\n");
				
				 switch (readChoice()) {
				   case 0: login(esql); break;
				   case 1: viewVideo(esql); break;
				   case 2: uploadVideo(esql); break;
				   case 3: deleteVideo(esql); break;
				   case 4: searchVideo(esql); break;
				   case 5: viewRecommendedVideos(esql); break;
				   case 6: popVideos(esql); break;
				   case 7: popchannels(esql); break;
				   case 8: createChannel(esql); break;
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
	
	/*
	 * if already logged in, sets username to default 'Not logged in'
	 * else Promts user to enter username and password
	 * Compares the input values with the user1 table
	 * If user is found, sets global username to the username and userid to uid from the table
	 * Set boolean loggedin to true 	
	*/
	public static void login(project esql) {
		
		try {
			
			if (!loggedin) {
				String uname;
				String password;
	        	String query = "";
	        	System.out.println("Enter Username: ");
	        	uname = in.readLine();
	        	System.out.println("Enter password: ");
	        	password = in.readLine();
	        	
	        	query = "SELECT * from user1 WHERE uname ='" + uname + "' AND password = '" + password + "';"  ;
	        	
	        	Statement stmt = _connection.createStatement();
	        	 
	 	        // issues the query instruction
	 	        ResultSet rs = stmt.executeQuery(query);
	 	        
	 	       rs.next();
	 	       
	 	       try {
	 	    	  if (uname.equals(rs.getString(4)) && password.equals(rs.getString(11))) {
		 	        	System.out.println("Successfully logged in");
		 	        	username = uname;
		 	        	userid = rs.getInt(1);
		 	        	loggedin = true;
		 	        }
		 	        else {
		 	        	System.out.println("Wrong username or password");
		 	        	return;
		 	        }
	 	       }
	 	       catch(Exception e) {
	 	    	   System.out.println("Wrong username or password");
	 	    	   return;
	 	       }
	 	       
	 	       
			}
			else {
				
				username = "Not logged in";
				loggedin = false;
				System.out.println("Successfully logged out");
			}
			
 	        
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
		
	}
	/*
	 * Searches video table for input string
	 * prints video id_s and titles of result
	 * promts user to select video id of desired video
	 * 
	 * needs to actually open the file
	 *
	*/
	public static void viewVideo(project esql) {
	     try {
	    	 	String input;
	    	 	String query = 	"";
	            
	            System.out.println("Enter video name");
	            
	            input = (in.readLine());
	            
	            query = "SELECT vin as video_id, title FROM video WHERE title ILIKE '%" + input + "%';";
	            		
	            
	            if(esql.executeQuery(query) == 0)
	            {
	            	System.out.println("No videos found");
	            }
	            else
	            {
	            	 System.out.println("Enter video id of the video to watch");
	            	 input = (in.readLine());
	            	 System.out.printf("Playing video %s\n", input);
	            	 System.out.printf("Actual video will be added later\n");
	            }
	            
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	
	/*
	 * Checks if the user is logged in 
	 * promts user to enter video information
	 * inserts into video table 
	 * 		increment channel numvids
	 * 
	 * needs to actually upload the video file
	 *
	*/
	public static void uploadVideo(project esql) {
	     try {
	            String query = 	"";
	            
	            if(!loggedin) {
	            	System.out.println("Must be logged in to upload a video");
	            	return;
	            }
	            else {
	            
	            	String vin = ""; //primary key
	            	int cid = 0; // channel id
	            	String desc = ""; //description
	            	String category = "";
	            	String title = "";
	            	Calendar calendar = Calendar.getInstance();
	                java.sql.Date curDate = new java.sql.Date(calendar.getTime().getTime());
	            	
	                      	
	            	// 1. get the vin
	            	// gets the current number of videos in the video table increments by 1 and sets it as 
		 	        // vid for the new video
	            		
	            	String query1 = "SELECT COUNT(vin) FROM video";
	            	Statement stmt = _connection.createStatement();
		 	        ResultSet rs = stmt.executeQuery(query1);
		 	        
		 	        rs.next();
		 	        
		 	        vin = Integer.toString(rs.getInt(1) + 1 ); 
		 	        
		 	        // 2. get cin
		 	        // find the channel of the current user channel table
		 	        // 			potential problem - user can have more than 1 channel
		 	        // 					for now will take the first one
		 	        try {
			 	   	String query2 = "SELECT cid FROM channel WHERE uid = " + userid +";";
		 	        rs = stmt.executeQuery(query2);
		 	        rs.next();
		 	        cid = rs.getInt(1); 
		 	        }
		 	        catch (Exception e) {
		 	        	System.out.println("You don't have a channel and can't upload a video without one");
		 	        	return;
		 	        }
		 	        
		 	        System.out.println("Enter video description (optional)");
		 	        desc = (in.readLine());
		 	        System.out.println("Enter video category (optional)");
		 	        category = (in.readLine());
		 	        System.out.println("Enter video title");
		 	        title = (in.readLine());
		 	        
		 	        // ready to insert into video table
		 	        // INSERT INTO table_name(column1, column2, …)
		 	        // VALUES (value1, value2, …);
		 	        query = "INSERT INTO video (vin, cid, uid, numLikes, numDislikes, numViews, description, category, title, publicationDate) "
		 	        		+ "VALUES ('" + vin + "', " + cid + ", " + userid + ", " + "0, 0, 0 ,'" + desc + "', '" + category + "', '" + title  + "', '" + curDate +"');";
		 	      
	            }
	            
	            esql.executeUpdate(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	
	/*
	 * prints all the videos that belong to the user
	 * Deletes the video that user indicates with vin After checking that video belongs to the user
	 * 
	 * */
	public static void deleteVideo(project esql) {
		try {
			if(!loggedin) {
            	System.out.println("Must be logged in to delete a video");
            	return;
            }
            else {
			
            	String query = 	"";
            	
    	        try {
			 	   	String query2 = "SELECT vin AS video_id, title FROM video WHERE uid = " + userid +";";
			 	    if(esql.executeQuery(query2) == 0) {
			 	    	 System.out.println("You don't have any videos");
			 	    	 return;
			 	    }
			 	    System.out.println("Enter the video id of the video you want to delete");
			 	  	String vin = (in.readLine());
			 	  	
			 	  	try {
			 	  		
			 	  		String query3 = "SELECT uid from video WHERE vin ='"+ vin + "';"  ;
			        	
			        	Statement stmt = _connection.createStatement();
			        	 
			 	        // issues the query instruction
			 	        ResultSet rs = stmt.executeQuery(query3);
			 	        
			 	        rs.next();
			 	       
			 	      
			 	    	if (userid == rs.getInt(1)) {
			 	    		System.out.printf("Deleting video %s \n", vin);
			 	    		query = "DELETE FROM video WHERE vin = '" + vin + "';";
			 	    		esql.executeUpdate(query);
			 	    		System.out.printf("Video %s deleted!\n", vin );
			 	    	}
			 	    	else {
			 	    		System.out.println("The selected video is not yours");
			 	    	}
				 	        
			 	  		
			 	  	}
			 	  	catch (Exception e) {
			 	  		System.out.println("Something went wrong");
			 	  		return;
			 	  	}
    	        }
		 	        catch (Exception e) {
		 	        	System.out.println("You don't any vidoes");
		 	        	return;
		 	        }
    	        
            }
			
	        
		} 
		catch (Exception e) {
			System.err.println(e.getMessage());
	    }
		
	}
	public static void searchVideo(project esql) {
	     try {
	    		String input;
	    	 	String query = 	"";
	            
	            System.out.println("Enter video name");
	            
	            input = (in.readLine());
	            
	            query = "SELECT vin as video_id, title FROM video WHERE title ILIKE '%" + input + "%';";
	            		
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	
	/*
	 * Finds most liked videos matching favorite category of the user
	 * */
	public static void viewRecommendedVideos(project esql) {
	     try {
	           
	            
	            if(!loggedin) {
	            	System.out.println("Not logged in");
	            	return;
	            }
	            else {
	            	
	            	// get users favorite category
	            	
	            	String fav_cat = "";
	            	
	            	String query1 = "SELECT fav_cat from user1 where uid = " + userid + ";";
	            	
	            	Statement stmt = _connection.createStatement();
		 	        ResultSet rs = stmt.executeQuery(query1);
		 	        rs.next();
	            	fav_cat = rs.getString(1);
	            	
	            	String query = "SELECT vin AS video_id, title FROM video WHERE category = '" + fav_cat + "' ORDER BY numlikes LIMIT 20;";
	            	esql.executeQuery(query);
	            }
	            	
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	public static void popVideos(project esql) {
	     try {
	    	 
	            String query = 	"SELECT vin as video_id, title FROM video ORDER BY (numlikes-numdislikes) DESC LIMIT 20;";
	            System.out.println("20 Most popular videos\n");
	            
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	public static void popchannels(project esql) {
	     try {
	    	 	String query = 	"SELECT cname as channel_name FROM channel ORDER BY (numsubs) DESC LIMIT 20;";
	            System.out.println("20 Most popular channels\n");
	            
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
	
	public static void createChannel(project esql) {
	     try {
	    	 	String query = 	"SELECT cname as channel_name FROM channel ORDER BY (numsubs) DESC LIMIT 20;";
	            System.out.println("20 Most popular channels\n");
	            
	            esql.executeQuery(query);
	 
	        } catch (Exception e) {
	            System.err.println(e.getMessage());
	        }
		
	}
}
