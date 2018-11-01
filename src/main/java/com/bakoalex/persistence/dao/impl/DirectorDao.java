package com.bakoalex.persistence.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.bakoalex.persistence.dao.Dao;
import com.bakoalex.persistence.database.ConnectionFactory;
import com.bakoalex.persistence.dto.Director;
import com.bakoalex.logger.ConsoleLogger;

public class DirectorDao implements Dao<Director> {

    private static final Logger LOGGER = ConsoleLogger.attach();

    /**
     * Check if the given Director object exists in the directors table
     */
    private Director isDirectorExistsInTable(Connection conn, Director director) throws SQLException {
        String queryString = "SELECT * FROM directors WHERE directors.director_id=? OR directors.director_name=?;";
        PreparedStatement pStatement = conn.prepareStatement(queryString);
        ResultSet queryResult = pStatement.executeQuery();
        if (queryResult.next()) return deserializeDirector(queryResult);
        else return null;
    }

    /**
     * Deserialize Director Object from a ReultSet.
     * @param result
     * @return Director object
     * @throws SQLException
     */
    private Director deserializeDirector(ResultSet result) throws SQLException {
        Director director = null;
        director = new Director(result.getInt(1), result.getString(2));
        return director;
    }

    /**
     * Returns a Director object from the directors table based on its director_id.
     */
    @Override
    public Director get(int id) {
        Director director = null;

        // Connecting to the database.
        // If the connection is not possible, we return a null-object.
        try (Connection conn = ConnectionFactory.getConnection()) {

            // Running the query
            String sqlQueryString = "SELECT * FROM directors WHERE directors.director_id=?;";
            PreparedStatement pStatement = conn.prepareStatement(sqlQueryString);
            pStatement.setInt(1, id);
            ResultSet sqlQuery = pStatement.executeQuery(sqlQueryString);
            
            // If there is no response, then something went wrong during the query.
            // Only one row should be in the result, so we check only the first row.
            if (sqlQuery.first()) director = deserializeDirector(sqlQuery);
            else return null;

        } catch (SQLException ex) {
            return null;
        }
        return director;
    }

    /**
     * Returns all Director records from the directors table as a List of Director objects.
     */
    @Override
    public List<Director> getAll() {
        List<Director> directors = new ArrayList<>();

        // Connecting to the database.
        // If the result is empty, we return an empty list.
        try (Connection conn = ConnectionFactory.getConnection()) {

            //Run the query
            String queryString = "SELECT * FROM directors;";
            Statement statement = conn.createStatement();
            ResultSet queryResult = statement.executeQuery(queryString);

            // Insert the data from the query to the list
            while (queryResult.next()) directors.add(new Director(queryResult.getInt(1), queryResult.getString(2)));

        } catch (SQLException ex) {
            LOGGER.info("SQL Exception: " + ex.getMessage());
            return directors;
        }
        return directors;
    }

    /**
     * Insert a new Dierctor record into the directors table.
     * The method returns the ID of the newly created record.
     * It only inserts a new record, if it is not a duplicate.
     * In case of any error, it returns 0.
     */
    @Override
    public int insert(Director director) {
        // Return 0 instantly if the input is null
        if (director == null) return 0;

        // Connecting to database
        try (Connection conn = ConnectionFactory.getConnection()) {

            // First we check if it is a duplicate
            if (isDirectorExistsInTable(conn, director) != null) return 0;
            
            // Now we can start adding the passed object to the database
            String queryString = "INSERT INTO directors (name) VALUES (?);";
            conn.setAutoCommit(false);
            PreparedStatement pStatement = conn.prepareStatement(queryString, PreparedStatement.RETURN_GENERATED_KEYS);
            pStatement.setString(1, director.getName());

            // If not 1 row was effected furing the query, then something went wrong.
            // We roll back our changes, and return 0.
            int affectedRows = pStatement.executeUpdate();
            if (affectedRows != 1) {
                conn.rollback();
                return 0;
            }

            // Getting the generated keys for the newly created record.
            ResultSet generatedKeys = pStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                conn.commit();
                return generatedKeys.getInt(1);
            }
            else return 0;

        } catch (SQLException ex) {
            LOGGER.info("SQLException: " + ex.getMessage());
            return 0;
        }
    }

    @Override
    public boolean update(Director director) {
        // Return false if the input is null
        if (director == null) return false;

        // Connecting to database
        try (Connection conn = ConnectionFactory.getConnection()) {

            // Check if the provided Director object exists in the database
            Director originalDirector = null;
            if ((originalDirector = isDirectorExistsInTable(conn, director)) == null) return false;

            // Check if the record in the table is the same as the provided one. 
            // If they are identical, there is no need to update anything, so we return false
            if (director.equals(originalDirector)) {
                LOGGER.info("The same record can be found in the database, ignore update operation");
                return false;
            }

            // Now we can run the update query
            String queryString = "UPDATE directors SET directors.director_name=? WHERE directors.director_id=?;";
            conn.setAutoCommit(false);
            PreparedStatement pStatement = conn.prepareStatement(queryString);
            pStatement.setString(1, director.getName());
            pStatement.setInt(2, director.getDirectorId());

            // If the count of affected rows different from 1, then something totally went wrong.
            // we rollback our opration, and we return false.
            int affectedRows = pStatement.executeUpdate();
            if (affectedRows != 1) {
                LOGGER.info("More than 1 row affected, aborting update operation");
                conn.rollback();
                return false;
            }

            conn.commit();
            return true;

        } catch (SQLException ex) {
            LOGGER.severe("SQLException: " + ex.getMessage());
            return false;
        }
    }

    @Override
    public boolean delete(Director director) {
        // If the input is null, we return false
        if (director == null) return false;

        try (Connection conn = ConnectionFactory.getConnection()) {
            // Now we check if the Director object exists in the database.
            // If it is not exists, we have nothing to do, we return false.
            if (isDirectorExistsInTable(conn, director) == null) return false;

            // Now we can delete the requested record from the table
            String queryString = "DELETE FROM directors WHERE directors.director_id=? OR directors.name=?;";
            conn.setAutoCommit(false);
            PreparedStatement pStatement = conn.prepareStatement(queryString);
            pStatement.setInt(1, director.getDirectorId());
            pStatement.setString(2, director.getName());

            // If there is not one row affected, something went wrong during the query.
            // We rollback our changes, and then return false.
            int affectedRows = pStatement.executeUpdate();
            if (affectedRows != 1) {
                LOGGER.severe("Unable to complete delete operation. More than one row affected. Rolling back.");
                conn.rollback();
                return false;
            }

            conn.commit();
            return true;

        } catch (SQLException ex) {
            LOGGER.severe("SQLExceptio: " + ex.getMessage());
            return false;
        }
    }
}