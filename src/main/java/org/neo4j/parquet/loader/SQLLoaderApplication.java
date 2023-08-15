package org.neo4j.parquet.loader;

import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Loads SQL data into the database via the driver
 */
@CommandLine.Command(name = "neo4j-load-sql", mixinStandardHelpOptions = true)
public final class SQLLoaderApplication extends AbstractCliApplication implements Callable<Integer> {

	@Option(
		names = {"--label"},
		description = "The label to use for the nodes (existing nodes with that label will be deleted)",
		required = true
	)
	private String label;

	@Option(
		names = {"--jdbc-url"},
		description = "The JDBC URL",
		required = true
	)
	private String jdbcUrl;

	@CommandLine.Parameters
	private String query;

	public static void main(String... args) {

		CommandLine commandLine = new CommandLine(new SQLLoaderApplication());
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);

		int exitCode = commandLine.execute(args);
		System.exit(exitCode);
	}

	@Override
	public Integer call() throws Exception {

		System.err.printf("Using %s with a batch size of %d, creating nodes with the label %s%n", BufferedNeo4jWriter.Mode.SERVER_SIDE_BATCHING, batchSize, label);

		try (
			var driver = GraphDatabase.driver(address, AuthTokens.basic(user, new String(password)));
			var session = driver.session(SessionConfig.forDatabase(database));
		) {
			session.run("""
				MATCH (p) WHERE $label IN labels(p)
				CALL {
				    WITH p DETACH DELETE p
				} IN TRANSACTIONS OF $rows ROWS""", Map.of("label", label, "rows", batchSize)).consume();

			var writer = new BufferedNeo4jWriter(session, batchSize, BufferedNeo4jWriter.Mode.SERVER_SIDE_BATCHING, label);

			var start = System.currentTimeMillis();
			// This is the jdbc interaction.
			try (
				var connection = DriverManager.getConnection(jdbcUrl);
				var stmt = connection.createStatement();
				var result = stmt.executeQuery(query);
			) {
				var metaData = result.getMetaData();
				// Iterating the result
				while (result.next()) {
					var newNodeProperties = new HashMap<String, Object>();
					// Iterate over all columns in the result
					for (int i = 1; i <= metaData.getColumnCount(); ++i) {
						// Map column name to property name, turn the column value into a neo4j value
						newNodeProperties.put(metaData.getColumnLabel(i), toNeo4jValue(result.getObject(i)));
					}
					// Add this to a custom writer
					writer.addNode(Values.value(newNodeProperties));
				}
			} // Everything opened in the try() {} block will be closed automatically

			writer.flush();

			var counters = writer.getCounters();
			var end = System.currentTimeMillis();
			System.err.printf("Added %d labels, created %d nodes, set %d properties, completed after %d ms%n", counters.labelsAdded(), counters.nodesCreated(), counters.propertiesSet(), end - start);
		}
		return 0;
	}

	private static Value toNeo4jValue(Object o) {
		if (o instanceof Timestamp ts) {
			return Values.value(ts.toLocalDateTime());
		} else if (o instanceof Date d) {
			return Values.value(d.toLocalDate());
		}
		// Hope for the best
		return Values.value(o);
	}
}
