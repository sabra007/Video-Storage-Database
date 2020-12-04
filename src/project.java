import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Calendar;

public class project
{

	public static void HDFSdownloadVideo(String vidID) throws Exception{


		//Add catch to catch invalid id such as not digits
        /* if (args.length != 2) {
        //    System.out.println("Invalid amount of Arguments");
         //   System.exit(0);


        }*/


		//find a way to make the destinations neater? if not its fine it just downloads to my workspace folder
		//for easy testing

		String destination[] = new String [2];
		destination[0] = "hdfs://localhost:9000/home/" + vidID + ".mp4";
		//destination[0] = "hdfs://localhost:9000/home/" + vidID
		destination[1] = "file:///Users/luis/Workspace/youtube/lsanc044/" + vidID + ".mp4";
		Configuration conf = new Configuration();
		try {

			// Hadoop DFS Path - Input & Output file
			Path inputPath = new Path(destination[0]);
			Path outputPath  = new Path(destination[1]);
			FileSystem in_fs = inputPath.getFileSystem(conf);
			FileSystem out_fs = outputPath .getFileSystem(conf);

			// Verification
			if (!in_fs.isFile(inputPath)) {
				System.out.println("Input file not found");
				throw new IOException("Input file not found");
			}
			if (out_fs.isFile(outputPath )) {
				System.out.println("Output file already exists");
				throw new IOException("Output file already exists");
			}

			// open and read from file
			FSDataInputStream in_stream = in_fs.open(inputPath);
			// Create file to write
			FSDataOutputStream out_stream = out_fs.create(outputPath);

			byte buffer[] = new byte[4096];
			long b_count = 0;
			long starting_Time = System.nanoTime();
			double elapsed_Time_Second = 0;
			try {
				int bytesRead = 0;
				while ((bytesRead = in_stream.read(buffer)) > 0) {
					out_stream.write(buffer, 0, bytesRead);
					b_count += bytesRead;
				}
			} catch (IOException e) {
				System.out.println("Error while copying file");
			} finally {
				in_stream.close();
				out_stream.close();
			}
			long running_Time = System.nanoTime() - starting_Time;
			elapsed_Time_Second = (double) running_Time / 1000000000;
			System.out.println("download " + b_count + " bytes from " + "server" + " to "
					+ "local" + " in " + elapsed_Time_Second + " seconds");

		} catch (IOException e) {
			e.printStackTrace();
		}


	}
	public static void HDFSuploadVideo(String vidID, String title) throws Exception{


		//Add catch to catch invalid id such as not digits
        /* if (args.length != 2) {
        //    System.out.println("Invalid amount of Arguments");
         //   System.exit(0);


        }*/



		//destination[0] = from
		//destination[1] = upload
		String destination[] = new String [2];
		//for title do we want to have user enter name of file or just make it that their file must
		//be the same name as their video title name
		destination[0] = "file:///Users/luis/Workspace/youtube/lsanc044/" + title + ".mp4";
		destination[1] = "hdfs://localhost:9000/home/" + vidID + ".mp4";
		Configuration conf = new Configuration();
		try {

			// Hadoop DFS Path - Input & Output file
			Path inputPath = new Path(destination[0]);
			Path outputPath  = new Path(destination[1]);
			FileSystem in_fs = inputPath.getFileSystem(conf);
			FileSystem out_fs = outputPath .getFileSystem(conf);

			// Verification
			if (!in_fs.isFile(inputPath)) {
				System.out.println("Input file not found");
				throw new IOException("Input file not found");
			}
			if (out_fs.isFile(outputPath )) {
				System.out.println("Output file already exists");
				throw new IOException("Output file already exists");
			}

			// open and read from file
			FSDataInputStream in_stream = in_fs.open(inputPath);
			// Create file to write
			FSDataOutputStream out_stream = out_fs.create(outputPath);

			byte buffer[] = new byte[4096];
			long b_count = 0;
			long starting_Time = System.nanoTime();
			double elapsed_Time_Second = 0;
			try {
				int bytesRead = 0;
				while ((bytesRead = in_stream.read(buffer)) > 0) {
					out_stream.write(buffer, 0, bytesRead);
					b_count += bytesRead;
				}
			} catch (IOException e) {
				System.out.println("Error while copying file");
			} finally {
				in_stream.close();
				out_stream.close();
			}
			long running_Time = System.nanoTime() - starting_Time;
			elapsed_Time_Second = (double) running_Time / 1000000000;
			System.out.println("Uploaded " + b_count + " bytes from " + "local" + " to "
					+ "server" + " in " + elapsed_Time_Second + " seconds");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
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

			System.out.println("(1)");

			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}


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
				System.out.println("9. Create an account");
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
					case 9: createAccount(esql); break;
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
				System.out.println(rs.getString(4));
				System.out.println(rs.getString(10));




				try {
					if (uname.equals(rs.getString(4)) && password.equals(rs.getString(10))) {
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
				HDFSdownloadVideo(input);
				//System.out.printf("Actual video will be added later\n");
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

				int vin; //primary key
				int cid = 0; // channel id
				String desc = ""; //description
				String category = "";
				String title = "";
				Calendar calendar = Calendar.getInstance();
				java.sql.Date curDate = new java.sql.Date(calendar.getTime().getTime());


				// 1. get the vin
				// gets the current number of videos in the video table increments by 1 and sets it as
				// vid for the new video

				String query1 = "SELECT MAX(vin) FROM video";
				Statement stmt = _connection.createStatement();
				ResultSet rs = stmt.executeQuery(query1);

				rs.next();

				vin = rs.getInt(1) + 1;

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

				if(title.isEmpty())
				{
					System.out.println("Can't upload a video without a title");
					return;
				}

				// ready to insert into video table
				// INSERT INTO table_name(column1, column2, â€¦)
				// VALUES (value1, value2, â€¦);
				query = "INSERT INTO video (vin, cid, uid, numLikes, numDislikes, numViews, description, category, title, publicationDate) "
						+ "VALUES (" + vin + ", " + cid + ", " + userid + ", " + "0, 0, 0 ,'" + desc + "', '" + category + "', '" + title  + "', '" + curDate +"');";
				HDFSuploadVideo(String.valueOf(vin), title);
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

						String query3 = "SELECT uid from video WHERE vin ="+ vin + ";"  ;

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

			if(!loggedin) {
				System.out.println("Log in to create a channel");
				return;
			}
			else {

				int cid = 0;
				String cname = "";

				// 1. get the new cid
				// gets the current number of channels in the channel table increments by 1 and sets it as
				// cid for the new channel

				String query1 = "SELECT MAX(cid) FROM channel";
				Statement stmt = _connection.createStatement();
				ResultSet rs = stmt.executeQuery(query1);

				rs.next();

				cid = rs.getInt(1) + 1;

				System.out.println("Enter channel name");
				cname = (in.readLine());

				String query = "INSERT INTO channel (cid, numSubs, numLikes, cname, cage, uid) "
						+ "VALUES (" + cid + ", 0, 0, '" + cname + "', 0, " + userid + ");";
				esql.executeUpdate(query);

				System.out.println("Channel Created");
			}

		} catch (Exception e) {
			System.err.println(e.getMessage());
		}

	}

	public static void createAccount(project esql) {

		try {

			if(loggedin) {
				System.out.println("Logout to create a new account");
				return;
			}
			else {
				// getting the next uid
				String query1 = "SELECT MAX(uid) FROM user1";
				Statement stmt = _connection.createStatement();
				ResultSet rs = stmt.executeQuery(query1);

				rs.next();

				userid = rs.getInt(1) + 1;

				System.out.println("Enter first name");
				String fname = (in.readLine());
				System.out.println("Enter last name");
				String lname = (in.readLine());
				System.out.println("Enter phone number");
				String number = (in.readLine());
				System.out.println("Enter your favorite category");
				String favCat = (in.readLine());
				System.out.println("Enter your age");
				int age = Integer.parseInt(in.readLine());
				System.out.println("Enter your address");
				String address = (in.readLine());
				System.out.println("Enter you email");
				String email = (in.readLine());

				System.out.println("Enter your username");
				username = (in.readLine());

				System.out.println("Enter your password");
				String password = (in.readLine());


				String query = String.format("INSERT INTO user1 (uid, fname, lname, uname, phone, address, email, age, fav_cat, password) "
						+ "VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s', %d, '%s', '%s');", userid, fname, lname, username, number, address, email, age, favCat, password);


				esql.executeUpdate(query);
				System.out.println("Account Created");
				loggedin = true;
			}






		} catch (Exception e) {
			System.err.println(e.getMessage());
		}

	}




} // end of class App
