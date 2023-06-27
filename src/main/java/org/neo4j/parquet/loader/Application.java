package org.neo4j.parquet.loader;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.SessionConfig;

import blue.strategic.parquet.ParquetReader;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Loads Parquet file into the database via the driver
 */
@CommandLine.Command(name = "neo4j-load-parquet", mixinStandardHelpOptions = true)
public final class Application implements Callable<Integer> {

	@Option(
		names = {"-a", "--address"},
		description = "The address of the Neo4j host.",
		required = true,
		defaultValue = "bolt://localhost:7687"
	)
	private URI address;

	@Option(
		names = {"-u", "--username"},
		description = "The login of the user connecting to the database.",
		required = true,
		defaultValue = "neo4j"
	)
	private String user;

	@Option(
		names = {"-p", "--password"},
		description = "The password of the user connecting to the database.",
		required = true,
		defaultValue = "verysecret"
	)
	private char[] password;

	@Option(
		names = {"--mode"},
		description = "Using server side or client side batching",
		required = true,
		defaultValue = "SERVER_SIDE_BATCHING"
	)
	private BufferedNeo4jWriter.Mode mode;

	@Option(
		names = {"--batch-size"},
		description = "Batch size to use",
		required = true,
		defaultValue = "50000"
	)
	private int batchSize;

	@Option(
		names = {"--label"},
		description = "The label to use for the nodes (existing nodes with that label will be deleted)",
		required = true
	)
	private String label;

	@Option(
		names = {"--database"},
		description = "The target database",
		required = true,
		defaultValue = "neo4j"
	)
	private String database;

	@CommandLine.Parameters
	File file;

	public static void main(String... args) {

		CommandLine commandLine = new CommandLine(new Application());
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);

		int exitCode = commandLine.execute(args);
		System.exit(exitCode);
	}

	@Override
	public Integer call() throws Exception {

		System.err.printf("Using %s with a batch size of %d, creating nodes with the label %s%n", mode, batchSize, label);

		try (
			var driver = GraphDatabase.driver(address, AuthTokens.basic(user, new String(password)));
			var session = driver.session(SessionConfig.forDatabase(database));
		) {
			var start = System.currentTimeMillis();
			var writer = new BufferedNeo4jWriter(session, batchSize, mode, label);

			session.run("""
				MATCH (p) WHERE $label IN labels(p)
				CALL {
				    WITH p DETACH DELETE p
				} IN TRANSACTIONS OF $rows ROWS""", Map.of("label", label, "rows", batchSize)).consume();
			var data = ParquetReader.streamContent(file, PropertiesHydrator::new);
			data.forEach(writer::addNode);
			writer.flush();

			var counters = writer.getCounters();
			var end = System.currentTimeMillis();
			System.err.printf("Added %d labels, created %d nodes, set %d properties, completed after %d ms%n", counters.labelsAdded(), counters.nodesCreated(), counters.propertiesSet(), end - start);
		}
		return 0;
	}
}
