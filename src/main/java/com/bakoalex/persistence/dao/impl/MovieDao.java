package com.bakoalex.persistence.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import com.bakoalex.persistence.dao.Dao;
import com.bakoalex.persistence.dao.exceptions.DaoItemDuplicate;
import com.bakoalex.persistence.dao.exceptions.DaoItemNotFound;
import com.bakoalex.persistence.dao.exceptions.DaoItemOperationFailed;
import com.bakoalex.persistence.database.ConnectionFactory;
import com.bakoalex.persistence.dto.Actor;
import com.bakoalex.persistence.dto.Director;
import com.bakoalex.persistence.dto.Movie;
import com.bakoalex.logger.ConsoleLogger;

public class MovieDao implements Dao<Movie> {

    // Custom logger
    private static final Logger log = ConsoleLogger.attach();
    // Used to maintain database connection
    private final Connection conn;

    /**
     * Default constructor, it sets the conn variable
     */
    public MovieDao() {
        this.conn = ConnectionFactory.getConnection();
    }

    /**
     * Based on the description of the task, when we check if a movie exists in the database
     * we have to care about only the title and the directors of the movies. All the other
     * data can be the same for a movie, but not these.
     * @param title Title of the Movie
     * @param directors List of the Dierctor objects for the given movie
     * @return A Movie object if found; otherwise it will return null
     * @throws SQLException when the connection was unsuccessful
     */
    private Movie getMovieByTitleAndDirectors(String title, List<Director> directors) throws SQLException {
        return null;
    }

    /**
     * Check if given Movie object is exists in the movies table
     * @param conn Connection object to the database
     * @param movie Movie object, which we want to check
     * @return An empty object if the movie doesnt exists, otherwise it returns a deserialized Movie object
     * @throws SQLException
     */
    private Movie isMovieExistsInTalbe(Connection conn, Movie movie) throws SQLException {
        String queryString = "SELECT * FROM movies WHERE movies.movies_id=? OR movies.title=?;";
        PreparedStatement pStatement = conn.prepareStatement(queryString);
        pStatement.setInt(1, movie.getMovieId());
        pStatement.setString(2, movie.getTitle());
        ResultSet queryResult = pStatement.executeQuery();
        if (queryResult.next()) return deserializeMovie(queryResult);
        else return null;
    }

    /**
     * Check if the given movie object exists in the movies table.
     * @param conn Connection object to database
     * @param movie Movie object
     * @return true if the given movie object exists in the talbe; otherwise will return false
     * @throws SQLException
     */
    private boolean isMovieExists(Connection conn, Movie movie) throws SQLException {
        // Check if the movie exists in the table by its title
        String queryString =    "SELECT movie_id FROM movies m WHERE m.title=?;";
        log.fine("Running query: " + queryString);
        PreparedStatement pStatement = conn.prepareStatement(queryString);
        pStatement.setString(1, movie.getTitle());

        // If the movie cannot be found by its title, we return false
        ResultSet queryResult = pStatement.executeQuery();
        if (!queryResult.next()) return false;

        // Check the directors
        queryString =           "SELECT * FROM directors d " + 
                                "JOIN movies_directors md ON d.director_id=md.director_id " +
                                "WHERE md.movie_id=?;";

        log.fine("Execute query: " + queryString);

        pStatement = conn.prepareStatement(queryString);
        pStatement.setInt(1, movie.getMovieId());

        List<Director> directorsFromQuery = new ArrayList<>();
        queryResult = pStatement.executeQuery();
        
        while (queryResult.next()) directorsFromQuery.add(new Director(queryResult.getInt(1), queryResult.getString(2)));

        if (movie.getDirectors().size() != directorsFromQuery.size()) return false;
        
        List<Director> directors = movie.getDirectors();

        // Sort both array by the name of the directors.
        Collections.sort(directors);
        Collections.sort(directorsFromQuery);

        for (int i=0; i<directors.size(); i++) {
            if (directors.get(i) != directorsFromQuery.get(i)) return false;
        }

        return true;
    }

    /**
     * Check if a Director object can be found in the database 
     * @param name Name of the director
     * @return Director object or null if not found.
     * @throws SQLException
     */
    private Director getDirectorByName(String name) throws SQLException {
        String queryString = "SELECT * FROM directors d WHERE d.name LIKE ?;";
        
        PreparedStatement pStatement = conn.prepareStatement(queryString);
        pStatement.setString(1, name);

        ResultSet result = pStatement.executeQuery();
        if (result.next()) return new Director(result.getInt(1), result.getString(2));

        return null;
    }

    /**
     * Check if an Actor object can be found in the database
     * @param name Name of the actor
     * @return Actor object or null, if not found.
     * @throws SQLException
     */
    private Actor getActorByName(String name) throws SQLException {
        String queryString = "SELECT * FROM actors a WHERE a.name LIKE ?;";

        PreparedStatement pStatement = conn.prepareStatement(queryString);
        pStatement.setString(1, name);

        ResultSet result = pStatement.executeQuery();
        if (result.next()) return new Actor(result.getInt(1), result.getString(2));

        return null;
    }

    /**
     * Create Movie object from a ResultSet object
     * @param result ResultSet object, which contains the SQL Query output
     * @return Movie object
     * @throws SQLException
     */
    private Movie deserializeMovie(ResultSet result) throws SQLException {
        Movie movie = new Movie();
        movie.setMovieId(result.getInt("movie_id"));
        movie.setTitle(result.getString("title"));
        movie.setYear(result.getInt("year"));
        movie.setLength(result.getInt("length"));
        movie.setMediaType(result.getString("media_type"));
        movie.setMediaCover(result.getString("media_cover"));
        movie.setMediaOrigin(result.getString("media_origin"));
        movie.setNumOfRents(result.getInt("num_of_rents"));
        movie.setRented(result.getBoolean("is_rented"));
        return movie;
    }

    /**
     * Get a Movie object from the movies table by it ID.
     * We return a null object if the requested Movie does not exists.
     * Otherwise, we return a de-serialized Movie object
     */
    public Movie get(int id) {

        // Connect to database
        try (Connection conn = ConnectionFactory.getConnection()) {
            // Initialize empty Movie object
            Movie movie = null;

            // Prepare queries
            String movieQueryString =       "SELECT * FROM movies m WHERE m.movie_id=?;";

            String actorsQueryString =      "SELECT a.actor_id, a.name FROM actors a " + 
                                            "INNER JOIN movies_actors ma ON a.actor_id=ma.actor_id " + 
                                            "WHERE ma.movie_id=?;";

            String directorsQueryString =   "SELECT d.director_id, d.name FROM directors d " + 
                                            "INNER JOIN movies_directors md ON d.director_id=md.director_id " + 
                                            "WHERE md.movie_id=?;";
            
            PreparedStatement mPreparedStatement = conn.prepareStatement(movieQueryString);
            PreparedStatement aPreparedStatement = conn.prepareStatement(actorsQueryString);
            PreparedStatement dPreparedStatement = conn.prepareStatement(directorsQueryString);

            mPreparedStatement.setInt(1, id);
            aPreparedStatement.setInt(1, id);
            dPreparedStatement.setInt(1, id);

            // First take care about Movie query
            ResultSet queryResult = mPreparedStatement.executeQuery();
            if (queryResult.next()) movie = deserializeMovie(queryResult);
            else throw new DaoItemNotFound("Movie not found in the database with movie_id of " + id);

            // Take care about actors
            queryResult = aPreparedStatement.executeQuery();
            if (queryResult.next()) {
                List<Actor> actors = new ArrayList<>();
                actors.add(new Actor(queryResult.getInt(1), queryResult.getString(2)));
                while (queryResult.next()) actors.add(new Actor(queryResult.getInt(1), queryResult.getString(2)));
                movie.setActors(actors);
            } else throw new DaoItemNotFound("Actor not found for movie with movie_id of " + id);

            // Take care about directors
            queryResult = dPreparedStatement.executeQuery();
            if (queryResult.next()) {
                List<Director> directors = new ArrayList<>();
                directors.add(new Director(queryResult.getInt(1), queryResult.getString(2)));
                while (queryResult.next()) directors.add(new Director(queryResult.getInt(1), queryResult.getString(2)));
                movie.setDirectors(directors);
            } else throw new DaoItemNotFound("Director not found for movie with movie_id of " + id);

            return movie;

        } catch (SQLException ex) {
            log.info("SQL Exception: " + ex.getMessage());
            return null;
        } catch (DaoItemNotFound ex) {
            log.severe(ex.getMessage());
            return null;
        }
    }

    /**
     * Return all Movie object from the database.
     * If no object was found, we return an empty list instead.
     * @return
     */
    public List<Movie> getAll() {
        //*******************NOTE**********************
        // This is not the optimal solution, since we
        // run 3 query for each Movie object which will
        // be very slow in case of lots of records.
        //*********************************************
        List<Movie> movies = new ArrayList<>();

        // Connecting to database
        try (Connection conn = ConnectionFactory.getConnection()) {

            // Run the query
            String queryString = "SELECT movie_id FROM movies;";
            ResultSet queryResult = conn.createStatement().executeQuery(queryString);

            Movie movie = null;
            while (queryResult.next()) {
                movie = get(queryResult.getInt(1));
                if (movie != null) movies.add(movie);
            }

            return movies;

        } catch (SQLException ex) {
            log.info("SQL Exception: " + ex.getMessage());
            return null;
        }
    }

    /**
     * Creates a new record in the database which represenets a Movie object.
     * In case of any error we return 0.
     * If the insert was successful, we return the id of the inserted movie record.
     */
    public int insert(Movie movie) {
        // If provided Movie object is null, we return 0
        if (movie == null) {
            log.info("Provided object is null.");
            return 0;
        }

        try (Connection conn = ConnectionFactory.getConnection()) {
            //Check if there is a movie with the same title and same director(s)
            if (isMovieExists(conn, movie)) throw new DaoItemDuplicate("Object is already in the database, cannot insert it again.");

            // Varirables for generated IDs
            int movieGeneratedKeys = 0;
            int actorGeneratedKeys = 0;
            int directorGeneratedKeys = 0;

            // Check which actors should be inserted
            List<String> actorsToBeInserted = new ArrayList<>();

            // Prepare SQL statements
            String movieSqlQuery =      "INSERT INTO movies (title, year, length, media_type, media_cover, media_origin, num_of_rents, is_rented) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?);";
            conn.setAutoCommit(false);

            //==================================================================================
            // NOTE: Jelenleg ott tartunk, hogy tudjuk, hogy a filmet be kell szurni.
            // A rendezok beszurasarol, es a foszereplok beszurasarol egyelore meg nem tudunk
            //==================================================================================

            PreparedStatement mPreparedStatement = conn.prepareStatement(movieSqlQuery,     PreparedStatement.RETURN_GENERATED_KEYS);

            // Prepare query statement for inserting a new Movie object into the movies table
            mPreparedStatement.setString(   1,  movie.getTitle());
            mPreparedStatement.setInt(      2,  movie.getYear());
            mPreparedStatement.setInt(      3,  movie.getLength());
            mPreparedStatement.setString(   4,  movie.getMediaType());
            mPreparedStatement.setString(   5,  movie.getMediaCover());
            mPreparedStatement.setString(   6,  movie.getMediaOrigin());
            mPreparedStatement.setInt(      7,  movie.getNumOfRents());
            mPreparedStatement.setInt(      8,  movie.isRented() ? 1 : 0);

            // Executing query for inserting new Movie
            int queryAffectedRows = mPreparedStatement.executeUpdate();

            // Gather generated keys for Movie object
            ResultSet mGeneratedKeys = mPreparedStatement.getGeneratedKeys();
            if (mGeneratedKeys.next()) movieGeneratedKeys = mGeneratedKeys.getInt(1);
            else {
                conn.rollback();
                return 0;
            }

            // Take care about actors
            // For each actor, we will try to insert them, but if we cant, we will try to get 
            // ther id from the database.
            ActorDao actorDao = new ActorDao();
            List<Integer> generatedLeyList = new ArrayList<>();

            for (Actor a : movie.getActors()) {
                Integer generatedKey = actorDao.insert(a);
                // If we couldnt insert the Actor to the database, because it already exists,
                // we have to get the id
                if (generatedKey == 0) {
                    //Prepare query
                    String queryString = "SELECT actor_id FROM actors a WHERE a.name LIKE ?;";
                    PreparedStatement pStatement = conn.prepareStatement(queryString);
                    pStatement.setString(1, a.getName());

                    // Run the query
                    ResultSet queryResult = pStatement.executeQuery();

                    // Get the ID from the existing Actor object
                    if (queryResult.next()) generatedKey = queryResult.getInt(1);
                    else {
                        conn.rollback();
                        return 0;
                    }
                }
                generatedLeyList.add(generatedKey);
            }

            // Take care about directors
            // For each director we will try to insert them to the database.
            // If the insertion fail, it means the director already exists in the table.
            // In this case, we have to get the Id of the existing record.
            DirectorDao directorDao = new DirectorDao();
            generatedLeyList = new ArrayList<>();
            Integer generatedKey = 0;
            PreparedStatement pStatement;
            ResultSet directorQueryResult;

            String directorQuery = "SELECT director_id FROM directors d WHERE d.name LIKE ?;";

            for (Director d : movie.getDirectors()) {
                // First we will try to insert the current director object to the database
                generatedKey = directorDao.insert(d);
                // If the generatedKey is still 0 means the director already exists, so we have to obtain its id.
                if (generatedKey == 0) {
                    pStatement = conn.prepareStatement(directorQuery);
                    pStatement.setString(1, d.getName());
                    directorQueryResult = pStatement.executeQuery();

                    // Check if there is something in the output of the query
                    if (directorQueryResult.next()) {
                        generatedKey = directorQueryResult.getInt(1);
                    } else {
                        conn.rollback();
                        throw new DaoItemOperationFailed("Could not insert Director nor get the id.");
                    }
                }
            }

            // Insert records into movies_actors joining table
            int insertCount = 0;
            String queryString = "INSERT INTO movies_actors (movie_id, actor_id) VALUES (?, ?);";
            for (Integer actorId : generatedLeyList) {
                pStatement = conn.prepareStatement(queryString);
                pStatement.setInt(1, movieGeneratedKeys);
                pStatement.setInt(2, actorId);

                int affectedRowCount = pStatement.executeUpdate();
                if (affectedRowCount == 1) insertCount++;
            }

            // If there was no insertion to actors table nor to the movies_actors table
            // means the insert was unsuccessful, we have to rollback, and return 0.
            if (insertCount == 0 && generatedLeyList.size() == 0) {
                conn.rollback();
                return 0;
            }

            conn.commit();
            return movieGeneratedKeys;

        } catch (SQLException ex) {
            log.info("SQL Exception: " + ex.getMessage());
        } catch (DaoItemDuplicate ex) {
            log.severe(ex.getMessage());
        } catch (DaoItemOperationFailed ex) {
            log.severe(ex.getMessage());
        }
        return 0;
    }

    public boolean update(Movie movie) {
        
        // Get the original record from the table
        Movie originalMovie = get(movie.getMovieId());

        if (originalMovie == null) {
            log.info("Could not found any macthing movies in the database with the movies_id of " + movie.getMovieId());
            return false;
        }

        if (movie.equals(originalMovie)) {
            log.fine("The same record can be found in the database.");
            return false;
        }

        log.finest("Connecting to database...");
        try (Connection conn = ConnectionFactory.getConnection()) {

            conn.setAutoCommit(false);

            String updateSQlString = " UPDATE movies " + 
                                     " SET movies.title=?, movies.year=?, movies.length=?, movies.media_type=?, movies.media_cover=?, " + 
                                     " movies.media_origin=?, movies.num_of_rents=?, movies.is_rented=? " +
                                     " WHERE movie_id=?; ";

            String updateActorsSqlString =  " UPDATE actors a " +
                                            " INNER JOIN movies_actors ma ON a.actor_id = ma.actor_id " + 
                                            " INNER JOIN movies m ON ma.movie_id=m.movie_id " +
                                            " SET a.actor_name=? " +
                                            " WHERE a.actor_id=? AND m.movie_id=?;";

            String updateDirectorSqlString =    " UPDATE directors d " +
                                                " INNER JOIN movies_directors md ON d.director_id=md.director_id " +
                                                " INNER JOIN movies m ON m.movie_id=md.movie_id " +
                                                " SET d.director_name=? " + 
                                                " WHERE d.director_id=? AND m.movie_id=?;";

            log.fine("SQL update string is: " + updateSQlString);

            PreparedStatement sql = conn.prepareStatement(updateSQlString);
            sql.setString(1, movie.getTitle());
            sql.setInt(2, movie.getYear());
            sql.setInt(3, movie.getLength());
            sql.setString(4, movie.getMediaType());
            sql.setString(5, movie.getMediaCover());
            sql.setString(6, movie.getMediaOrigin());
            sql.setInt(7, movie.getNumOfRents());
            sql.setInt(8, movie.isRented() ? 1 : 0);
            sql.setInt(9, movie.getMovieId());

            log.finest("Running sql query: " + sql.toString());
            sql.execute();
            conn.commit();
            log.finest("SQL query completed.");
            
        } catch (SQLException ex) {
            log.info("Could not update the movie: " + movie.toString() + ", exception message: " + ex.getMessage());
            return false;
        }
        return false;
    }

    public boolean delete(Movie movie) {
        return false;
    }
}