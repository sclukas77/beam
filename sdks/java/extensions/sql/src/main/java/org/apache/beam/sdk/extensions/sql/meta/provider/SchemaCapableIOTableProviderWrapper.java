package org.apache.beam.sdk.extensions.sql.meta.provider;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.io.InvalidSchemaException;
import org.apache.beam.sdk.schemas.io.SchemaCapableIOProvider;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.values.Row;

import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;

public abstract class SchemaCapableIOTableProviderWrapper extends InMemoryMetaTableProvider {
    private SchemaCapableIOProvider schemaCapableIOProvider;

    public SchemaCapableIOTableProviderWrapper(SchemaCapableIOProvider schemaCapableIOProvider) {
        this.schemaCapableIOProvider = schemaCapableIOProvider;
    }

    @Override
    public String getTableType() {
        return schemaCapableIOProvider.identifier();
    }

    @Override
    public BeamSqlTable buildBeamSqlTable(Table tableDefinition) {
        JSONObject tableProperties = tableDefinition.getProperties();

        try {
            RowJson.RowJsonDeserializer deserializer =
                    RowJson.RowJsonDeserializer.forSchema(schemaCapableIOProvider.configurationSchema())
                            .withNullBehavior(RowJson.RowJsonDeserializer.NullBehavior.ACCEPT_MISSING_OR_NULL);

            Row configurationRow =
                    newObjectMapperWith(deserializer).readValue(tableProperties.toString(), Row.class);

            SchemaIO schemaIO =
                    schemaCapableIOProvider.from(
                            tableDefinition.getLocation(), configurationRow, tableDefinition.getSchema());

            return SchemaIOTableWrapper.fromSchemaIO(schemaIO);
        } catch (InvalidConfigurationException | InvalidSchemaException e) {
            throw new InvalidTableException(e.getMessage());
        } catch (JsonProcessingException e) {
            throw new AssertionError(
                    "Failed to re-parse TBLPROPERTIES JSON " + tableProperties.toString());
        }

    }

}
