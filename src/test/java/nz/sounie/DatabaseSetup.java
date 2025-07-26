package nz.sounie;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseSetup {
    public static void createTable(Connection connection) throws SQLException {
        // TIDB doesn't support UUID as a data type, so working with varchar instead
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE event (id BINARY(16) PRIMARY KEY, " +
                                    "name varchar(255) NOT NULL," +
                                    "version bigint NOT NULL DEFAULT 0)");
        }
    }

    public static void dropTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE event");
        }
    }
}
