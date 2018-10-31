package com.bakoalex.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.bakoalex.database.ConnectionFactory;
import com.bakoalex.dto.Actor;
import com.bakoalex.logger.ConsoleLogger;

public class ActorDao implements Dao<Actor> {

    private static final Logger LOGGER = ConsoleLogger.attach();

    /**
     * Deserialize Actor object from an SQL ResultSet 
     * @param result
     * @return Actor object
     * @throws SQLException
     */
    private Actor deserializeActor(ResultSet result) throws SQLException {
        if (result == null) return null;
        Actor actor = new Actor();
        actor.setActorId(result.getInt(1));
        actor.setName(result.getString(2));
        return actor;
    }

    /**
     * Returns an Actor object by its actor_id from the database, only if exists.
     */
    @Override
    public Actor get(int id) {
        Actor actor = null;

        LOGGER.fine(ConsoleLogger.DB_CONN);
        try (Connection conn = ConnectionFactory.getConnection()) {
            LOGGER.fine(ConsoleLogger.DB_CONN_OK);
            
            String sqlQueryString = "SELECT * FROM actors WHERE actors.actor_id=" + id + ";";
            LOGGER.fine(ConsoleLogger.DB_QUERY + sqlQueryString);

            Statement statement = conn.createStatement();
            ResultSet sqlQuery = statement.executeQuery(sqlQueryString);
            
            if (sqlQuery.first()) {
                // Only one record should be matched. It returns a new Actor object from the ResultSet.
                actor = deserializeActor(sqlQuery);
            } else {
                // Return null if there is no mathcing record
                LOGGER.info(ConsoleLogger.DB_QUERY_EMPTY);
                return null;
            }

        } catch (SQLException ex) {
            LOGGER.info(ConsoleLogger.SQL_EXC + ex.getMessage());
            return null;
        }
        return actor;
    }

    /**
     * Returns all records from the actors table as a List of Actor objects.
     */
    @Override
    public List<Actor> getAll() {
        List<Actor> actors = new ArrayList<>();

        // Connect to the database.
        // If there is a problem while connecting, we return an empty list.
        try(Connection conn = ConnectionFactory.getConnection()) {

            // Run the query
            String queryString = "SELECT * FROM actors;";
            LOGGER.fine(ConsoleLogger.DB_QUERY + queryString);
            Statement statement = conn.createStatement();
            ResultSet sqlQuery = statement.executeQuery(queryString);

            // Insert all the data from the query result to the list.
            while (sqlQuery.next()) actors.add(deserializeActor(sqlQuery));

        } catch (SQLException ex) {
            LOGGER.info("SQLExceptio: " + ex.getMessage());
            return actors;
        }

        return actors;
    }

    /**
     * Insert an Actor object to the actors table
     * Returns the inserted row's actor_id.
     */
    @Override
    public int insert(Actor actor) {

        LOGGER.fine(ConsoleLogger.DB_CONN);
        try (Connection conn = ConnectionFactory.getConnection()) {
            LOGGER.fine(ConsoleLogger.DB_CONN_OK);

            //Check if the same actor is in the database
            String sqlQueryString = "SELECT * FROM actors WHERE actors.name LIKE ?;";
            LOGGER.fine(ConsoleLogger.DB_QUERY + sqlQueryString);
            PreparedStatement pStatement = conn.prepareStatement(sqlQueryString);
            pStatement.setString(1, actor.getName());

            // If the result is not empty, then it is a duplicate, we dont add anything
            ResultSet queryResult = pStatement.executeQuery();
            if (queryResult.first()) {
                LOGGER.info(ConsoleLogger.DB_QUERY_DUPL + actor.toString());
                return 0;
            }

            // Now we can add our Actor object to the database
            sqlQueryString = "INSERT INTO actors (name) VALUES (?);";
            LOGGER.fine(ConsoleLogger.DB_QUERY + sqlQueryString);

            conn.setAutoCommit(false);
            pStatement = conn.prepareStatement(sqlQueryString, PreparedStatement.RETURN_GENERATED_KEYS);
            pStatement.setString(1, actor.getName());

            int affectedRows = pStatement.executeUpdate();
            if (affectedRows != 1) {
                LOGGER.info(ConsoleLogger.DB_INSERT_FAIL + actor.toString());
                conn.rollback();
                return 0;
            }

            conn.commit();
            LOGGER.fine(ConsoleLogger.DB_INSERT_OK + actor.toString());

            //Return the generated actor_id
            ResultSet generatedId = pStatement.getGeneratedKeys();
            if (generatedId.next()) return generatedId.getInt(1);
            else return 0;


        } catch (SQLException ex) {
            LOGGER.info(ConsoleLogger.SQL_EXC + ex.getMessage());
            return 0;
        }
    }

    /**
     * Updates an existing record in the actors tables based on the provided Actor object.
     * If the correspondant record does not exists in the table, returns false.
     * Return true only if the update was successfull.
     */
    @Override
    public boolean update(Actor actor) {
        LOGGER.fine(ConsoleLogger.DB_CONN);
        try (Connection conn = ConnectionFactory.getConnection()) {
            LOGGER.fine(ConsoleLogger.DB_CONN_OK);
            
            // Check if the provided Actor object exists in the actors table.
            String sqlQueryString = "SELECT * FROM actors WHERE actors.actor_id=?;";
            LOGGER.fine(ConsoleLogger.DB_QUERY + sqlQueryString);
            PreparedStatement pStatement = conn.prepareStatement(sqlQueryString);
            pStatement.setInt(1, actor.getActorId());

            // If the result set is empty, we return false, otherwise we will compare the result
            // with the passed object. If they are not differ there is no need for a query.
            ResultSet queryResult = pStatement.executeQuery();
            Actor actorFromTheDb = null;
            if (!queryResult.first()) {
                LOGGER.info(ConsoleLogger.DB_QUERY_EMPTY);
                return false;
            } else {
                actorFromTheDb = deserializeActor(queryResult);
            }

            // If the passed actor is the same as in the database, we return false.
            if (actor.equals(actorFromTheDb)) {
                LOGGER.info(ConsoleLogger.DB_QUERY_DUPL + actor.toString());
                return false; 
            }

            // Now we can start the update process.
            sqlQueryString = "UPDATE actors SET actors.actor_id=?, actors.name=? WHERE actors.actor_id=?;";
            LOGGER.fine(ConsoleLogger.DB_QUERY + sqlQueryString);
            conn.setAutoCommit(false);
            pStatement = conn.prepareStatement(sqlQueryString);
            pStatement.setInt(1, actor.getActorId());
            pStatement.setString(2, actor.getName());
            pStatement.setInt(3, actor.getActorId());

            // If there isnt one row affected, something is wrong
            int affectedRows = pStatement.executeUpdate();
            if (affectedRows != 1) {
                LOGGER.info(ConsoleLogger.DB_UPDATE_FAIL + actor.toString());
                conn.rollback();
                return false;
            }

            conn.commit();
            LOGGER.fine(ConsoleLogger.DB_UPDATE_OK + actor.toString());
            return true;

        } catch (SQLException ex) {
            LOGGER.info(ConsoleLogger.SQL_EXC + ex.getMessage());
            return false;
        }
    }

    /**
     * Deletes an existing record from the actors table.
     * If the provided Actor object doesnt exists in the table, or there is an error during the 
     * SQL operation we return false.
     * If the deletion was successfull, we return true.
     */
    @Override
    public boolean delete(Actor actor) {
        LOGGER.fine(ConsoleLogger.DB_CONN);
        try (Connection conn = ConnectionFactory.getConnection()) {
            LOGGER.fine(ConsoleLogger.DB_CONN_OK);

            // Check if the provided Actor object exists in the database
            String sqlQueryString = "SELECT * FROM actors WHERE actors.actor_id=?;";
            PreparedStatement pStatement = conn.prepareStatement(sqlQueryString);
            pStatement.setInt(1, actor.getActorId());

            // If the the result is empty we return false
            ResultSet queryResult = pStatement.executeQuery();
            if (!queryResult.first()) return false;

            // Now we can delete the object
            sqlQueryString = "DELETE FROM actors WHERE actors.actor_id=?;";
            conn.setAutoCommit(false);
            pStatement = conn.prepareStatement(sqlQueryString);
            pStatement.setInt(1, actor.getActorId());

            // If the affected row count is not 1 then something went wrong
            int affectedRows = pStatement.executeUpdate();
            if (affectedRows != 1) return false;

            conn.commit();
            return true;

        } catch (SQLException ex) {
            LOGGER.info(ConsoleLogger.SQL_EXC + ex.getMessage());
            return false;
        }
    }
}