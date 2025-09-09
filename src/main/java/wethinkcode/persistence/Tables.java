package wethinkcode.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Tables {
    private final Connection connection;

    public Tables(Connection connection) {
        this.connection = connection;
    }

    public boolean createGenres() {
        String sql = """
            CREATE TABLE Genres (
                code TEXT NOT NULL PRIMARY KEY,
                description TEXT NOT NULL
            )
        """;
        return createTable(sql);
    }



public boolean createBooks() {
    // Make sure Genres exists first
    createGenres(); 

    String sql = """
        CREATE TABLE Books (
            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            title TEXT NOT NULL,
            genre_code TEXT NOT NULL REFERENCES Genres(code)
        )
    """;
    return createTable(sql);
}


    protected boolean createTable(String sql) {
        if (!sql.trim().toUpperCase().startsWith("CREATE TABLE")) {
            return false; // only CREATE TABLE allowed
        }
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
