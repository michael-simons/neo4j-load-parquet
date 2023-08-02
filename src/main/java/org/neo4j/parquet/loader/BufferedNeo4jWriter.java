package org.neo4j.parquet.loader;

import java.io.Flushable;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.summary.SummaryCounters;

/**
 * A writer that buffers values upto a certain batch size and then creates nodes with a specified label and the given values as properties.
 * Batches are either done locally or with an {@code UNWIND} call remotely.
 */
class BufferedNeo4jWriter implements Flushable {

	record Counters(
		int labelsAdded,
		int nodesCreated,
		int releationshipsCreated,
		int propertiesSet
	) {

		Counters() {
			this(0, 0, 0, 0);
		}

		Counters(SummaryCounters counters) {
			this(counters.labelsAdded(), counters.nodesCreated(), counters.relationshipsCreated(), counters.propertiesSet());
		}

		Counters plus(Counters rhs) {
			return new Counters(labelsAdded + rhs.labelsAdded(), nodesCreated + rhs.nodesCreated(), releationshipsCreated + rhs.releationshipsCreated(), propertiesSet + rhs.propertiesSet());
		}

		Counters plus(SummaryCounters rhs) {
			return new Counters(labelsAdded + rhs.labelsAdded(), nodesCreated + rhs.nodesCreated(), releationshipsCreated + rhs.relationshipsCreated(), propertiesSet + rhs.propertiesSet());
		}
	}

	public interface WriteStrategy {

		Counters write(Session session, String label, List<Value> values);
	}

	enum Mode implements WriteStrategy {
		CLIENT_SIDE_BATCHING {
			@Override
			public Counters write(Session session, String label, List<Value> values) {
				var query = new Query("CREATE (n:%s) SET n = $properties".formatted(label));
				var result = new Counters();
				try (var tx = session.beginTransaction()) {
					for (Value value : values) {
						result = result.plus(tx.run(query.withParameters(Values.parameters("properties", value))).consume().counters());
					}
					tx.commit();
				}
				return result;
			}
		},
		SERVER_SIDE_BATCHING {
			@Override
			public Counters write(Session session, String label, List<Value> values) {
				var query = new Query("UNWIND $data AS properties CREATE (n:%s) SET n = properties".formatted(label));
				try (var tx = session.beginTransaction()) {
					var result = new Counters(tx.run(query.withParameters(Values.parameters("data", values))).consume().counters());
					tx.commit();
					return result;
				}
			}
		}
	}

	private final Session session;
	private final int batchSize;
	private final WriteStrategy mode;
	private final List<Value> buffer;
	private final String label;
	private Counters counters = new Counters();

	public BufferedNeo4jWriter(Session session, int batchSize, WriteStrategy writeStrategy, String label) {

		this.session = session;
		this.batchSize = batchSize;
		this.mode = writeStrategy;
		this.buffer = new ArrayList<>(batchSize);
		this.label = label;
	}

	public void addNode(Value properties) {
		buffer.add(properties);
		if (buffer.size() >= batchSize) {
			flush();
		}
	}

	@Override
	public void flush() {
		counters = counters.plus(mode.write(session, label, buffer));
		buffer.clear();
	}

	public Counters getCounters() {
		return counters;
	}
}
