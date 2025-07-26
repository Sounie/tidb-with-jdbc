package nz.sounie;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Self-contained worker object for upserting a specified version to a specified id.
 * <p>
 *     Intended to be used for trying out testing race conditions and the influence of transactions
 *     with different isolation levels.
 * </p>
 */
public class Upserter {
    final Connection connection;
    final PreparedStatement statement;
    final int version;

    boolean success = false;

    /**
     *
     * @param connection with isolation level set up, based on options available from Connection
     * @param idAsString primary key
     * @param name name of event
     * @param version
     */
    Upserter(Connection connection, String idAsString, String name, int version) {
        try {
            this.connection = connection;
            this.version = version;
            // TiDB doesn't appear to allow specifying a where clause for "on duplicate key update"
            // So we have an update with an ugly check for version on each individual property

            this.statement =  connection.prepareStatement(
                    """
INSERT INTO event (id, name, version) VALUES (UUID_TO_BIN(?), ?, ?)
ON DUPLICATE KEY UPDATE
    name = IF(version < VALUES(version), VALUES(name), name),
    version = IF(version < VALUES(version), VALUES(version), version)
"""
            );
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to prepare statement", e);
        }

        try {
            statement.setString(1, idAsString);
            statement.setString(2, name);
            statement.setInt(3, version);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to set up state for prepared statement.", e);
        }
    }

    void performUpsert()
    {
        boolean rowChanged = false;
        try {
            int rowsChanged = statement.executeUpdate();

            System.out.println("rows changed: " + rowsChanged);

            /* TODO: Verify whether the rowsChanged behaviour follows MySQL and MariaDB's approach,
            that can enable us to distinguish between whether the insert or update flow was involved.
            */
            if (rowsChanged > 0) {
                rowChanged = true;
            }

            connection.commit();

            this.success = true;
        } catch (SQLException e) {
            try {
                System.err.println("Failed to commit prepared statement, " + e.getMessage() + " Rolling back.");
                connection.rollback();
            }  catch (SQLException ex) {
                System.err.println("Failure during rollback, " + ex.getMessage());
            }
            // Don't need to do anything here, as success state will remain false.
        } finally {
            try {
                if (rowChanged) {
                    if (success) {
                        System.out.println("Version " + version + " inserted / updated");
                    }
                    else {
                        System.out.println("Version " + version + " inserted / updated, but connection closed without commit.");
                    }
                } else {
                    System.out.println("No row changed, presume version was already > " + version);
                }
                statement.close();
            } catch (SQLException e) {
                // We don't regard this as the upsert failing.
                System.err.println("Exception while closing statement: " + e.getMessage());
            }
        }
    }

    public void closeConnection() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            System.err.println("Exception when closing connection: " + e.getMessage());
        }
    }

    public boolean isSuccess() {
        return success;
    }
}
