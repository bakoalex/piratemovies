package com.bakoalex.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.bakoalex.database.ConnectionFactory;
import com.bakoalex.dto.Actor;
import com.bakoalex.dto.Director;
import com.bakoalex.dto.Movie;
import com.bakoalex.logger.ConsoleLogger;
import com.mysql.cj.xdevapi.Statement;

public class MovieDao implements Dao<Movie> {

    private static final Logger log = ConsoleLogger.attach();

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

            String actorsQueryString =      "SELECT a.actor_id, a.name FROM actors a INNER JOIN movies_actors ma ON a.actor_id=ma.actor_id " + 
                                            "INNER JOIN movies m ON m.movie_id=ma.movie_id WHERE m.movie_id=?;";

            String directorsQueryString =   "SELECT d.director_id, d.name FROM directors d INNER JOIN movies_directors md " + 
                                            "ON d.director_id=md.director_id INNER JOIN movies m ON m.movie_id=md.movie_id WHERE m.movie_id=?;";
            
            PreparedStatement mPreparedStatement = conn.prepareStatement(movieQueryString);
            PreparedStatement aPreparedStatement = conn.prepareStatement(actorsQueryString);
            PreparedStatement dPreparedStatement = conn.prepareStatement(directorsQueryString);

            mPreparedStatement.setInt(1, id);
            aPreparedStatement.setInt(1, id);
            dPreparedStatement.setInt(1, id);

            // First take care about Movie query
            ResultSet queryResult = mPreparedStatement.executeQuery();
            if (queryResult.next()) movie = deserializeMovie(queryResult);
            else return null;

            // Take care about actors
            queryResult = aPreparedStatement.executeQuery();
            if (queryResult.next()) {
                List<Actor> actors = new ArrayList<>();
                actors.add(new Actor(queryResult.getInt(1), queryResult.getString(2)));
                while (queryResult.next()) actors.add(new Actor(queryResult.getInt(1), queryResult.getString(2)));
                movie.setActors(actors);
            } else return null;

            // Take care about directors
            queryResult = dPreparedStatement.executeQuery();
            if (queryResult.next()) {
                List<Director> directors = new ArrayList<>();
                directors.add(new Director(queryResult.getInt(1), queryResult.getString(2)));
                while (queryResult.next()) directors.add(new Director(queryResult.getInt(1), queryResult.getString(2)));
                movie.setDirectors(directors);
            } else return null;

            return movie;

        } catch (SQLException ex) {
            log.info("SQL Exception: " + ex.getMessage());
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
            String queryString = "SELECT * FROM movies;";
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
        if (movie == null) return 0;

        try (Connection conn = ConnectionFactory.getConnection()) {
            //Check if there is a movie with the same title and same director(s)
            // NOTE: This should be changed, to check not only the title and the id, but also the directors.
            if (isMovieExistsInTalbe(conn, movie) == null) return 0;

            int movieGeneratedKeys = 0;
            int actorGeneratedKeys = 0;
            int directorGeneratedKeys = 0;

            // Prepare SQL statements
            String movieSqlQuery =      "INSERT INTO movies (title, year, length, media_type, media_cover, media_origin, num_of_rents, is_rented) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?);";
            String directorSqlQuery =   "INSERT INTO directors d " +
                                        "INNER JOIN movies_directors md ON d.director_id=md.director_id " +
                                        "INNER JOIN movies m ON m.movie_id=md.movie_id " +
                                        "";
            String actorSqlQuery =      "INSERT INTO actors (name) VALUES (?);";

            conn.setAutoCommit(false);

            PreparedStatement mPreparedStatement = conn.prepareStatement(movieSqlQuery, PreparedStatement.RETURN_GENERATED_KEYS);
            PreparedStatement dPreparedStatement = conn.prepareStatement(directorSqlQuery, PreparedStatement.RETURN_GENERATED_KEYS);
            PreparedStatement aPreparedStatement = conn.prepareStatement(actorSqlQuery, PreparedStatement.RETURN_GENERATED_KEYS);

            // First add the Movie object to the Movies table
            mPreparedStatement.setString(       1, movie.getTitle());
            mPreparedStatement.setInt(          2, movie.getYear());
            mPreparedStatement.setInt(          3, movie.getLength());
            mPreparedStatement.setString(       4, movie.getMediaType());
            mPreparedStatement.setString(       5, movie.getMediaCover());
            mPreparedStatement.setString(       6, movie.getMediaOrigin());
            mPreparedStatement.setInt(          7, movie.getNumOfRents());
            mPreparedStatement.setInt(          8, movie.isRented() ? 1 : 0);

            // Executing query for inserting new Movie
            if (!mPreparedStatement.execute()) {
                conn.rollback();
                return 0;
            }

            // Gather generated keys for Movie object
            ResultSet mGeneratedKeys = mPreparedStatement.getGeneratedKeys();
            if (mGeneratedKeys.next()) movieGeneratedKeys = mGeneratedKeys.getInt(1);
            else {
                conn.rollback();
                return 0;
            }

            // Now take car of actor(s)
            ActorDao actorDao = new ActorDao();
            for (Actor a : movie.getActors()) {
                // Check if the actor exists in the actors table
                if ((actorGeneratedKeys = actorDao.insert(a)) == 0) actorGeneratedKeys = a.getActorId();
                if (actorGeneratedKeys == 0) {
                    conn.rollback();
                    return 0;
                }
                // NOTE: Here we need DAO class for the movies_actors table!!!
                // Implement it ASAP!!!!
            }

            PreparedStatement sql = conn.prepareStatement("ASD");
            log.finest("Executing SQL Query: " + sql.toString());
            sql.execute();
            conn.commit();
            log.finest("SQL query completed successfully.");

            return 1;

        } catch (SQLException ex) {
            log.info("SQL Exception: " + ex.getMessage());
            return 0;
        }
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