package org.neo4j.parquet.loader;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.parquet.loader.BufferedNeo4jWriter.WriteStrategy;

import blue.strategic.parquet.ParquetReader;
import picocli.CommandLine;
import scala.Int;

/**
 * Loads a SO-dump converted to parquet (as described in the readme) into the database via the driver.
 */
@CommandLine.Command(name = "neo4j-load-so", mixinStandardHelpOptions = true)
public class SoLoaderApplication implements Callable<Integer> {
	@CommandLine.Option(
		names = {"-a", "--address"},
		description = "The address of the Neo4j host.",
		required = true,
		defaultValue = "bolt://localhost:7687"
	)
	private URI address;

	@CommandLine.Option(
		names = {"-u", "--username"},
		description = "The login of the user connecting to the database.",
		required = true,
		defaultValue = "neo4j"
	)
	private String user;

	@CommandLine.Option(
		names = {"-p", "--password"},
		description = "The password of the user connecting to the database.",
		required = true,
		defaultValue = "verysecret"
	)
	private char[] password;

	@CommandLine.Option(
		names = {"--batch-size"},
		description = "Batch size to use",
		required = true,
		defaultValue = "50000"
	)
	private int batchSize;

	@CommandLine.Option(
		names = {"--database"},
		description = "The target database",
		required = true,
		defaultValue = "neo4j"
	)
	private String database;

	@CommandLine.Parameters
	File file;

	public static void main(String... args) {

		CommandLine commandLine = new CommandLine(new SoLoaderApplication());
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);

		int exitCode = commandLine.execute(args);
		System.exit(exitCode);
	}

	@Override
	public Integer call() throws Exception {

		try (
			var driver = GraphDatabase.driver(address, AuthTokens.basic(user, new String(password)));
			var session = driver.session(SessionConfig.forDatabase(database));
		) {
			session.run("""
				MATCH (p:Post)
				CALL {
				    WITH p DETACH DELETE p
				} IN TRANSACTIONS OF $rows ROWS""", Map.of("rows", batchSize)).consume();
			session.run("""
				MATCH (u:User)
				CALL {
				    WITH u DETACH DELETE u
				} IN TRANSACTIONS OF $rows ROWS""", Map.of("rows", batchSize)).consume();
			session.run("""
				MATCH (p:Post)
				CALL {
				    WITH p DETACH DELETE p
				} IN TRANSACTIONS OF $rows ROWS""", Map.of("rows", batchSize)).consume();
			session.run("""
				CREATE CONSTRAINT user_id IF NOT EXISTS
				FOR (u:User) REQUIRE u.id IS UNIQUE
				""").consume();
			session.run("""
				CREATE CONSTRAINT post_id IF NOT EXISTS
				FOR (p:Post) REQUIRE p.id IS UNIQUE
				""").consume();
			session.run("""
				CREATE INDEX accepted_answer_id IF NOT EXISTS
				FOR (p:Post) ON p.accepted_answer_id
				""").consume();

			WriteStrategy writeStrategy = (s, label, values) -> {
				var query = new Query("""
					UNWIND $rows AS row
					MERGE (u:User {id: row.user_id})
					ON CREATE
					SET u.name = row.user_name,
					    u.reputation = row.user_reputation
					CREATE (p:Post {id: row.id, title_or_excerpt: row.title_or_excerpt, last_activity: row.last_activity_date, accepted_answer_id: row.accepted_answer_id})
					CREATE (p) -[:CREATED_BY {at: row.created_at}]-> (u)
					WITH row, p
					CALL {
						WITH row, p
						MATCH (pp:Post {id: row.parent_id})
						CREATE (p) -[:ANSWER_TO]-> (pp)
					}
					WITH p
					CALL {
						WITH p
						MATCH (p) -[a:ANSWER_TO]-> (pp:Post {accepted_answer_id: p.id})
						SET a.accepted = true
					}
					""");
				try (var tx = s.beginTransaction()) {
					var result = new BufferedNeo4jWriter.Counters(tx.run(query.withParameters(Values.parameters("rows", values))).consume().counters());
					tx.commit();
					return result;
				}
			};
			var writer = new BufferedNeo4jWriter(session, batchSize, writeStrategy, null);
			var data = ParquetReader.streamContent(file, listOfColumns -> new PropertiesHydrator<>(listOfColumns, Values::value));

			var start = System.currentTimeMillis();
			data.forEach(writer::addNode);
			writer.flush();

			var counters = writer.getCounters();
			var end = System.currentTimeMillis();
			System.err.printf("Added %d labels, created %d nodes and %d relationships, set %d properties, completed after %d ms%n", counters.labelsAdded(), counters.nodesCreated(), counters.releationshipsCreated(), counters.propertiesSet(), end - start);
		}
		return 0;
	}
}