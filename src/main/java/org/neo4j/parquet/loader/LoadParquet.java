package org.neo4j.parquet.loader;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import blue.strategic.parquet.ParquetReader;

public final class LoadParquet {

	@Procedure(name = "loadParquet")
	public Stream<MapResult> impl(@Name("resource") String resource) throws IOException {

		var uri = URI.create(resource);
		var scheme = uri.getScheme();

		File inputFile;
		if (scheme == null || "file".equalsIgnoreCase(scheme)) {
			inputFile = uri.isAbsolute() ? new File(uri) : new File(uri.getPath());
		} else {
			// Probably there's a better library than the one I picked that can deal with Parquet on remote hosts
			// Making that nice is not part of this PoC
			inputFile = File.createTempFile("neo4j-", ".parquet");
			try (
				var in = uri.toURL().openStream();
				var out = new BufferedOutputStream(new FileOutputStream(inputFile))) {
				in.transferTo(out);
			}
		}

		return ParquetReader.streamContent(inputFile, listOfColumns -> new PropertiesHydrator<>(listOfColumns, MapResult::new));
	}

	public record MapResult(Map<String, Object> row) {
	}
}
