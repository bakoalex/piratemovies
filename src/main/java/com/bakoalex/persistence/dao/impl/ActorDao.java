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
import com.bakoalex.persistence.dto.Actor;
import com.bakoalex.logger.ConsoleLogger;

public class ActorDao implements Dao<Actor> {

    private static final Logger log = ConsoleLogger.attach();

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
     * If the Actor doesnt exists, it return a null Actor object
     */
    @Override
    public Actor get(int id) {
        // Connecting to database
        try (Connection conn = ConnectionFactory.getConnection()) {

            // Prepare Query
            String queryString = "SELECT * FROM actors a WHERE a.actor_id=?;";
            PreparedStatement pStatement = conn.prepareStatement(queryString);
            pStatement.setInt(1, id);

            // Run the query
            ResultSet queryResult = pStatement.executeQuery();
            
            // De-Serialize Actor object from query result
            if (queryResult.next()) return new Actor(queryResult.getInt(1), queryResult.getString(2));
            else return null;

        } catch (SQLException ex) {
            log.info(ConsoleLogger.SQL_EXC + ex.getMessage());
            return null;
        }
    }

    /**
     * Returns all records from the actors table as a List of Actor objects.
     */
    @Override
    public List<Actor> getAll() {
        List<Actor> actors = new ArrayList<>();

        // Connect to the database.
        try(Connection conn = ConnectionFactory.getConnection()) {

            // Prepare the query
            String queryString = "SELECT * FROM actors;";
            Statement statement = conn.createStatement();

            // Run the query
            ResultSet queryResult = statement.executeQuery(queryString);

            // Gather all data from the query result
            while (queryResult.next()) actors.add(new Actor(queryResult.getInt(1), queryResult.getString(2)));
            // Return the list
            return actors;

        } catch (SQLException ex) {
            log.info("SQLException: " + ex.getMessage());
            return actors;
        }
    }

    /**
     * Insert an Actor object to the actors table
     * Returns the inserted row's actor_id.
     */
    @Override
    public int insert(Actor actor) {

        // If the input object is null, we return 0
        if (actor == null) return 0;
        
        // Connecting to database
        try (Connection conn = ConnectionFactory.getConnection()) {

            //Check if the same actor is in the database
            String queryString = "SELECT * FROM actors WHERE actors.name LIKE ?;";
            PreparedStatement pStatement = conn.prepareStatement(queryString);
            pStatement.setString(1, actor.getName());

            // If the result is not empty, then it is a duplicate, we dont add anything
            ResultSet queryResult = pStatement.executeQuery();
            if (queryResult.next()) return 0;

            // Insert the actor to database;
            conn.setAutoCommit(false);
            queryString = "INSERT INTO actors (name) VALUES (?);";
            pStatement = conn.prepareStatement(queryString, PreparedStatement.RETURN_GENERATED_KEYS);
            pStatement.setString(1, actor.getName());

            // Run the query, gather the created new ID
            int affectedRows = pStatement.executeUpdate();
            if (affectedRows != 1) {
                conn.rollback();
                return 0;
            }

            conn.commit();

            //Return the generated actor_id
            ResultSet generatedId = pStatement.getGeneratedKeys();
            if (generatedId.next()) return generatedId.getInt(1);
            else return 0;

        } catch (SQLException ex) {
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
        log.fine(ConsoleLogger.DB_CONN);
        try (Connection conn = ConnectionFactory.getConnection()) {
            log.fine(ConsoleLogger.DB_CONN_OK);
            
            // Check if the provided Actor object exists in the actors table.
            String sqlQueryString = "SELECT * FROM actors WHERE actors.actor_id=?;";
            log.fine(ConsoleLogger.DB_QUERY + sqlQueryString);
            PreparedStatement pStatement = conn.prepareStatement(sqlQueryString);
            pStatement.setInt(1, actor.getActorId());

            // If the result set is empty, we return false, otherwise we will compare the result
            // with the passed object. If they are not differ there is no need for a query.
            ResultSet queryResult = pStatement.executeQuery();
            Actor actorFromTheDb = null;
            if (!queryResult.first()) {
                log.info(ConsoleLogger.DB_QUERY_EMPTY);
                return false;
            } else {
                actorFromTheDb = deserializeActor(queryResult);
            }

            // If the passed actor is the same as in the database, we return false.
            if (actor.equals(actorFromTheDb)) {
                log.info(ConsoleLogger.DB_QUERY_DUPL + actor.toString());
                return false; 
            }

            // Now we can start the update process.
            sqlQueryString = "UPDATE actors SET actors.actor_id=?, actors.name=? WHERE actors.actor_id=?;";
            log.fine(ConsoleLogger.DB_QUERY + sqlQueryString);
            conn.setAutoCommit(false);
            pStatement = conn.prepareStatement(sqlQueryString);
            pStatement.setInt(1, actor.getActorId());
            pStatement.setString(2, actor.getName());
            pStatement.setInt(3, actor.getActorId());

            // If there isnt one row affected, something is wrong
            int affectedRows = pStatement.executeUpdate();
            if (affectedRows != 1) {
                log.info(ConsoleLogger.DB_UPDATE_FAIL + actor.toString());
                conn.rollback();
                return false;
            }

            conn.commit();
            log.fine(ConsoleLogger.DB_UPDATE_OK + actor.toString());
            return true;

        } catch (SQLException ex) {
            log.info(ConsoleLogger.SQL_EXC + ex.getMessage());
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
        log.fine(ConsoleLogger.DB_CONN);
        try (Connection conn = ConnectionFactory.getConnection()) {
            log.fine(ConsoleLogger.DB_CONN_OK);

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
            log.info(ConsoleLogger.SQL_EXC + ex.getMessage());
            return false;
        }
    }
}