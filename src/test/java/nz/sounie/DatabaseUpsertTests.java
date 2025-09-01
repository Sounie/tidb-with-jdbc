package nz.sounie;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.tidb.TiDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

// NB: Docker needs to be running in order for this test to be runnable.
@Testcontainers
class DatabaseUpsertTests {
    private static final String DB_USER = "root";
    private static final String PASSWORD = "";

    // Annotation applied to hook into TestContainers lifecycle management.
    @Container
    private static final TiDBContainer tidb = new TiDBContainer("pingcap/tidb");

    @Test
    void testTransactionIsolationLevel() throws Exception {
        // TiDB doesn't support read uncommitted, so we can't try that
        // https://docs.pingcap.com/tidb/stable/transaction-isolation-levels/
        int transactionReadCommited = Connection.TRANSACTION_READ_COMMITTED;

        assertThat(tidb.isRunning()).isTrue();
        assertThat(tidb.getHost()).isNotEmpty();
        assertThat(tidb.getFirstMappedPort()).isGreaterThan(0);

        String jdbcUrl = tidb.getJdbcUrl();
        System.out.println("JDBC URL: " + jdbcUrl);

        // Set up table
        try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD)) {
            DatabaseSetup.createTable(connection);
        }

        UUID id = UUID.randomUUID();
        String idAsString = id.toString();

        final int NUMBER_OF_UPSERTERS = 5;
        List<Upserter> upserters = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_UPSERTERS; i++) {
            Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD);
            connection.setAutoCommit(false);

            connection.setTransactionIsolation(transactionReadCommited);
            Upserter upserter = new Upserter(connection, idAsString, "First event", i + 1);
            upserters.add(upserter);
        }

        // Shuffle the ordering of elements in the upserters list
        Collections.shuffle(upserters);

        // Set up a concurrent executor to perform the upsert calls concurrently
        try (ExecutorService executorService =  Executors.newFixedThreadPool(NUMBER_OF_UPSERTERS,
            // Trying out use of VirtualThreads
                    (Runnable task) -> Thread.ofVirtual()
                                .unstarted(task)
                )) {
            for (Upserter upserter : upserters) {
                executorService.submit(() -> {
                        // Sleeping to allow ExecutorService to accumulate upserters before they run
                        try {
                            Thread.sleep(200L);
                        } catch (InterruptedException e) {
                            System.out.println("Sleep interrupted");
                        }
                        upserter.performUpsert();
                        if (!upserter.isSuccess()) {
                            System.err.println("Upsert failed");
                        }

                        upserter.closeConnection();
                    }
                );
            }
        }

        // Wait for all upserters to finish.)
        try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD)) {
            readRow(connection, id, 5);
        }
    }

    @AfterAll
    static void closeDownDatabaseSetup() throws Exception {
        String jdbcUrl = tidb.getJdbcUrl();
        System.out.println("JDBC URL: " + jdbcUrl);

        // Set up table
        try (Connection connection = DriverManager.getConnection(jdbcUrl, DB_USER, PASSWORD)) {
            DatabaseSetup.dropTable(connection);
        }
    }

    private void readRow(Connection connection, UUID expectedId, int expectedVersion) throws SQLException {
        try (PreparedStatement readStatement = connection.prepareStatement("SELECT BIN_TO_UUID(id) AS uuidId, name, version from event")) {
            // assert that one result and has name of "First event"
            readStatement.execute();

            try (ResultSet resultSet = readStatement.getResultSet()) {
                boolean firstResult = resultSet.next();
                assertThat(firstResult).isTrue();
                String idAsString = resultSet.getString("uuidId");
                int version = resultSet.getInt("version");
                assertThat(idAsString).isEqualTo(expectedId.toString());
                assertThat(version).isEqualTo(expectedVersion);
                assertThat(resultSet.getString("name")).isEqualTo("First event");

                // Verifying that no unexpected additional results are returned.
                boolean subsequentResult = resultSet.next();
                assertThat(subsequentResult).isFalse();
            }
        }
    }
}
