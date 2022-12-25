package com.airports.test;

import java.sql.*;
import java.util.Scanner;
import java.io.*;

public class FromFileToTable {


    private static final String ChurchRoad = null;

    public static Connection connectToDatabase(String user, String password,
                                               String database) {
        System.out.println("the user " + user);
        System.out.println("-------- PostgreSQL JDBC Connection Testing ------------");

        Connection connection = null;

        try {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/airport",
                    user, password);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
        }
        return connection;
    }


    public static void dropTable(Connection connection, String table) {
        Statement st = null;
        try {
            st = connection.createStatement();
            st.execute("DROP TABLE " + table);

            st.close();
        } catch (Exception e) {
            System.out.println("table doesnt exists creating a new table");

        }
    }

    public static void createTable(Connection connection,
                                   String tableDescription) {
        Statement st = null;
        try {
            System.out.println("this will now create a table");
            st = connection.createStatement();
            st.execute("CREATE TABLE " + tableDescription);
            st.close();
        } catch (Exception e) {
            System.out.println("Cant create table with the same table name");
        }
    }

    public static int insertIntoTableFromFileairport(Connection connection,
                                                     String table, String file) {
        System.out.println("in the insert table method airport");
        BufferedReader br = null;
        int numRows = 0;
        try {
            Statement st = connection.createStatement();
            String sCurrentLine, brokenLine[], composedLine = "";
            br = new BufferedReader(new FileReader(file));

            while ((sCurrentLine = br.readLine()) != null) {
// Insert each line to the DB
                brokenLine = sCurrentLine.split(",");
                composedLine = "INSERT INTO airport VALUES (";
                int i;
                for (i = 0; i < brokenLine.length - 1; i++) {
                    composedLine += "'" + brokenLine[i] + "',";
                }
                composedLine += "'" + brokenLine[i] + "')";
                numRows = st.executeUpdate(composedLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return numRows;
    }

    public static int insertIntoTableFromFileDelayedFlights(Connection connection,
                                                            String table, String file) {
        System.out.println("in the insert table method delayedFlights");
        BufferedReader br = null;
        int numRows = 0;
        try {
            Statement st = connection.createStatement();
            String sCurrentLine, brokenLine[], composedLine = "";
            br = new BufferedReader(new FileReader(file));

            while ((sCurrentLine = br.readLine()) != null) {
// Insert each line to the DB
                brokenLine = sCurrentLine.split(",");
                composedLine = "INSERT INTO delayedFlights VALUES (";
                int i;
                for (i = 0; i < brokenLine.length - 1; i++) {
                    composedLine += "'" + brokenLine[i] + "',";
                }
                composedLine += "'" + brokenLine[i] + "')";
                numRows = st.executeUpdate(composedLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return numRows;
    }

    public static void main(String[] argv) throws SQLException {


// No need for port number; just use default
        String port = "";


        Scanner input = new Scanner(System.in);
        System.out.println("enter your username: ");
        String username = input.next();
        Scanner input2 = new Scanner(System.in);
        System.out.println("enter your password: ");
        String password1 = input2.next();
        String DB_URL = "jdbc:mysql://localhost:3306/airport/";
        //System.out.println("jdbc:postgresql://teachdb.cs.rhul.ac.uk/CS2855/" + username);

        connectToDatabase(username, password1, DB_URL);

        Connection connection = connectToDatabase(username, password1, DB_URL);

        if (connection != null) {

            dropTable(connection, "airport");
            createTable(connection, "airport(airportCode varchar(15) primary key, airportName varchar(100), City varchar(50), State varchar(50));");
            int rowsAirport = insertIntoTableFromFileairport(connection, "airport",
                    "C:\\Users\\developer\\Downloads\\airportDataHW\\src\\main\\resources\\airport.sql");
            dropTable(connection, "delayedFlights");
            createTable(connection, "delayedFlights(ID_of_Delayed_Flight int primary key, Month int, DayofMonth int, DayOfWeek int, DepTime int, ScheduledDepTime int, ArrTime int, ScheduledArrTime int,UniqueCarrier varchar(15) , FlightNum int, ActualFlightTime int, scheduledFlightTime int, AirTime int, ArrDelay int, DepDelay int, Orig varchar(15), Dest varchar(15), Distance int);");
            int rowsDelayedFlights = insertIntoTableFromFileDelayedFlights(connection, "delayedFlights",
                    "C:\\Users\\developer\\Downloads\\airportDataHW\\src\\main\\resources\\delayedFlights.sql");

            System.out.println("####################1st Query#######################");
            String query1 = "SELECT uniquecarrier,"
                    + " COUNT(id_of_delayed_flight)"
                    + " FROM delayedFlights"
                    + " GROUP BY uniquecarrier"
                    + " ORDER BY COUNT(id_of_delayed_flight) DESC LIMIT 5";
            Statement st = connection.createStatement();
            //firstquery(connection,query1);
            ResultSet rs = st.executeQuery(query1);
            while (rs.next()) {
                System.out.print(rs.getString("uniquecarrier"));
                System.out.print("   ");
                System.out.println(rs.getInt(2));
            }
            rs.close();

            System.out.println("####################2nd Query#######################");
            String query2 = "SELECT airport.City,"
                    + " COUNT(*) AS DepDelaySum"
                    + " FROM delayedFlights"
                    + " INNER JOIN airport"
                    + " ON delayedflights.Orig = airport.airportCode"
                    + " GROUP BY airport.City"
                    + " ORDER BY DepDelaySum DESC LIMIT 5";

            ResultSet rs2 = st.executeQuery(query2);
            while (rs2.next()) {
                System.out.print(rs2.getString(1));
                System.out.print("  ");
                System.out.println(rs2.getInt(2));
            }
            rs2.close();

            System.out.println("####################3th Query#######################");
            String query3 = "SELECT DEST, SUM(ArrDelay) AS arrdelaytime"
                    + " FROM delayedFlights"
                    + " GROUP BY Dest"
                    + " ORDER BY arrdelaytime DESC LIMIT 5"
                    + " OFFSET 1";

            ResultSet rs3 = st.executeQuery(query3);
            while (rs3.next()) {
                System.out.print(rs3.getString(1));
                System.out.print("  ");
                System.out.println(rs3.getInt(2));
            }
            rs3.close();

            System.out.println("####################4th Query#######################");
            String query4 = "SELECT State, COUNT(airportCode) AS airportCount"
                    + " FROM airport"
                    + " GROUP BY State"
                    + " HAVING airportCount >= 10";

            ResultSet rs4 = st.executeQuery(query4);
            while (rs4.next()) {
                System.out.print(rs4.getString(1));
                System.out.print("  ");
                System.out.println(rs4.getInt(2));
            }
            rs4.close();
            st.close();
            connection.close();

        } else {
            System.out.println("Failed to make connection!");
            return;
        }

    }
}
