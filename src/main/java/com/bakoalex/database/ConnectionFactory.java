package com.bakoalex.database;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Logger;

import com.bakoalex.logger.ConsoleLogger;
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;

public class ConnectionFactory {

    private static MysqlConnectionPoolDataSource conn;

    private static final String DRIVER      = "com.mysql.cj.jdbc.Driver";
    private static final String HOST        = "localhost"; 
    private static final int    PORT        = 3306;
    private static final String DBNAME      = "piratemovies";
    private static final String USER        = "alexbako";
    private static final String PASS        = "asdQWE123";

    private static Map<String, String> tables;
    static {
        tables = new HashMap<>();
        tables.put("movies", "./resources/sql/tbl_movies.sql");
        tables.put("actors", "./resources/sql/tbl_actors.sql");
        tables.put("directors", "./resources/sql/tbl_directors.sql");
        tables.put("movies_actors", "./resources/sql/tbl_movies_actors.sql");
        tables.put("movies_directors", "./resources/sql/tbl_movies_directors.sql");
        tables.put("rents", "./resources/sql/tbl_rents.sql");
        tables.put("renter_persons", "./resources/sql/tbl_renter_persons.sql");
    }

    private static final Logger LOGGER = ConsoleLogger.attach();

    private ConnectionFactory() {}

    public static Connection getConnection() {
        if (conn == null) {
            try {
                Class.forName(DRIVER);
                LOGGER.fine("Using database driver: " + DRIVER);
                conn = new MysqlConnectionPoolDataSource();
                LOGGER.fine("Creating database connection using: ServerName=" 
                    + HOST + ":" + PORT + ", DatabaseName=" + DBNAME + ", UserName=" + USER + ", Password=*******"
                );

                conn.setServerName(HOST);
                conn.setPort(PORT);
                conn.setDatabaseName(DBNAME);
                conn.setUser(USER);
                conn.setPassword(PASS);

            } catch (ClassNotFoundException ex) {
                LOGGER.severe("Database driver couldn't be found. Please install the proper dirver! Message: " + ex.getMessage());
                LOGGER.severe("Aborting...");
                System.exit(1);
            }
        }

        try {
            Connection dbConnection = conn.getPooledConnection().getConnection();
            LOGGER.fine("Database connection successfull.");
            return dbConnection;
        } catch (SQLException ex) {
            LOGGER.severe("Cannot connect to the database. Reason: " + ex.getMessage());
            LOGGER.severe("Aborting...");
            System.exit(1);
        }
        return null;
    }

    public static void initializeTable(String table_name) {
        if (!tables.containsKey(table_name)) {
            LOGGER.severe("Table not found, could not initialize: " + table_name);
            return;
        }

        FileInputStream inputStream = null; 
        Scanner sc = null;
        StringBuilder sqlQuery = null;

        try {
            inputStream = new FileInputStream(tables.get(table_name));
            sc = new Scanner(inputStream, "UTF-8");
            sc.useDelimiter("(;(\r)?\n)|(--\n)");
            sqlQuery = new StringBuilder();
            
            while (sc.hasNextLine()) sqlQuery.append(sc.nextLine());

            if (sc.ioException() != null) {
                LOGGER.severe("Could not read file: " + tables.get(table_name));
                LOGGER.severe(sc.ioException().getMessage());
                sc.close();
                inputStream.close();
                return;
            }

            inputStream.close();
            sc.close();

        } catch (FileNotFoundException ex) {
            LOGGER.severe("Configuration file could not be found: " + tables.get(table_name));
            LOGGER.severe(ex.getMessage());
            sc.close();
            return;

        } catch (IOException ex) {
            LOGGER.severe("Could not read file: " + tables.get(table_name));
            LOGGER.severe(ex.getMessage());
            sc.close();
            return;

        }

        try (Connection db = getConnection()) {
            LOGGER.info("Preparing to drop table: " + table_name);
            
            PreparedStatement dropTable = db.prepareStatement("DROP TABLE ?;");
            dropTable.setString(1, table_name);
            LOGGER.finest("SQLQuery is: " + dropTable.toString());

            LOGGER.info("SQL Insturctions: " + sqlQuery.toString());

            if (! dropTable.execute()) {
                LOGGER.severe("Could not perform SQL Query. Dropping table '" + table_name + "' failed. Aborting operation.");
                LOGGER.severe(dropTable.getResultSet().toString());
                return;
            }
            LOGGER.info("Successfully dropped table: " + table_name);

            PreparedStatement createTable = db.prepareStatement(sqlQuery.toString());
            if (! createTable.execute()) {
                LOGGER.severe("Could not perform SQL Query. Creating table '" + table_name + "' failed. Aborting operation.");
                return;
            }
            LOGGER.info("Successfully created table: " + table_name);

        } catch (SQLException ex) {
            LOGGER.severe("SQLException: " + ex.getMessage());
            return;
        }

    }
}