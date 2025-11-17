/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.cdk.integrations.destination.jdbc

import io.airbyte.cdk.integrations.destination.async.function.DestinationFlushFunction
import io.airbyte.cdk.integrations.destination.async.model.PartialAirbyteMessage
import io.airbyte.cdk.integrations.destination.buffered_stream_consumer.RecordWriter
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.StreamDescriptor
import java.util.stream.Stream

class JdbcInsertFlushFunction(
    private val defaultNamespace: String,
    private val recordWriter: RecordWriter<PartialAirbyteMessage>,
    override val optimalBatchSizeBytes: Long
) : DestinationFlushFunction {
    @Throws(Exception::class)
    override fun flush(streamDescriptor: StreamDescriptor, stream: Stream<PartialAirbyteMessage>) {
        // Process stream in batches to avoid loading entire dataset into memory
        // This prevents OOM errors and reduces GC pressure
        val batchSize = 10000
        val streamPair = AirbyteStreamNameNamespacePair(
            streamDescriptor.name,
            streamDescriptor.namespace ?: defaultNamespace
        )

        val iterator = stream.iterator()
        val batch = mutableListOf<PartialAirbyteMessage>()

        while (iterator.hasNext()) {
            batch.add(iterator.next())
            if (batch.size >= batchSize) {
                recordWriter.accept(streamPair, batch.toList())
                batch.clear()
            }
        }

        // Flush remaining records
        if (batch.isNotEmpty()) {
            recordWriter.accept(streamPair, batch)
        }
    }
}
