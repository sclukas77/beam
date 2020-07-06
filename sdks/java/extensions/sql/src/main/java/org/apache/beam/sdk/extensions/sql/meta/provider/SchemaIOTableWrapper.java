package org.apache.beam.sdk.extensions.sql.meta.provider;

import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BaseBeamTable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;

public class SchemaIOTableWrapper extends BaseBeamTable implements Serializable {
    protected final SchemaIO schemaIO;

    private SchemaIOTableWrapper(SchemaIO schemaIO) {
        this.schemaIO = schemaIO;
    }

    static SchemaIOTableWrapper fromSchemaIO(SchemaIO schemaIO) {
        return new SchemaIOTableWrapper(schemaIO);
    }

    @Override
    public PCollection.IsBounded isBounded() {
        return PCollection.IsBounded.UNBOUNDED;
    }

    @Override
    public Schema getSchema() {
        return schemaIO.schema();
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
        PTransform<PBegin, PCollection<Row>> readerTransform = schemaIO.buildReader();
        return begin.apply(readerTransform);
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
        PTransform<PCollection<Row>, POutput> writerTransform = schemaIO.buildWriter();
        return input.apply(writerTransform);
    }

    @Override
    public BeamTableStatistics getTableStatistics(PipelineOptions options) {
        return BeamTableStatistics.UNBOUNDED_UNKNOWN;
    }
}
