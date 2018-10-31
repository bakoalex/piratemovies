package com.bakoalex.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.bakoalex.database.ConnectionFactory;
import com.bakoalex.dto.MovieActor;

public class MovieActorDao {

    private MovieActor deserializeMovieActor(ResultSet result) throws SQLException {
        return new MovieActor(result.getInt(1), result.getInt(2));
    }

    /**
     * Returns a row as a MovieActor object from the movies_actors table.
     * It return null if there is no row with the given movie_id
     * @param id movie_id of the MovieActor object
     * @return MovieActor object created from the query result
     */
    public MovieActor getByMovieId(int id) {
        // Connecting to the database
        try (Connection conn = ConnectionFactory.getConnection()) {

            // SQL Query String
            String sqlQueryString = "SELECT * FROM movies_actors ma WHERE ma.movie_id=?;";

            // Create statement
            PreparedStatement pStatement = conn.prepareStatement(sqlQueryString);
            pStatement.setInt(1, id);

            // Run the query
            ResultSet queryResult = pStatement.executeQuery();
            if (queryResult.next()) return deserializeMovieActor(queryResult);
            else return null;

        } catch (SQLException ex) {
            return null;
        }
    }

    public MovieActor getByActorId(int id) {
        return null;
    }

    /**
     * Returns all rows from the moivies_actors table as a List of MovieActor objects.
     * It returns an empty list if there are no records found.
     * @return List of MovieActor objects.
     */
    public List<MovieActor> getAll() {
        List<MovieActor> movieActors = null;

        return movieActors;
    }

    public int insert(MovieActor movieActor) {
        return 0;
    }

    public boolean update(MovieActor movieActor) {
        return false;
    }

    public boolean delete(MovieActor movieActor) {
        return false;
    }
}