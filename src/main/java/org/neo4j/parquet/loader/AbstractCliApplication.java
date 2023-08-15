package org.neo4j.parquet.loader;

import java.net.URI;

import picocli.CommandLine;

abstract class AbstractCliApplication {

	@CommandLine.Option(
		names = {"-a", "--address"},
		description = "The address of the Neo4j host.",
		required = true,
		defaultValue = "bolt://localhost:7687"
	)
	protected URI address;

	@CommandLine.Option(
		names = {"-u", "--username"},
		description = "The login of the user connecting to the database.",
		required = true,
		defaultValue = "neo4j"
	)
	protected String user;

	@CommandLine.Option(
		names = {"-p", "--password"},
		description = "The password of the user connecting to the database.",
		required = true,
		defaultValue = "verysecret"
	)
	protected char[] password;

	@CommandLine.Option(
		names = {"--batch-size"},
		description = "Batch size to use",
		required = true,
		defaultValue = "50000"
	)
	protected int batchSize;

	@CommandLine.Option(
		names = {"--database"},
		description = "The target database",
		required = true,
		defaultValue = "neo4j"
	)
	protected String database;
}
