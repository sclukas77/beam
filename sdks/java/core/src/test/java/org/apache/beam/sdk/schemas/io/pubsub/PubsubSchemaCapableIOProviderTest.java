package org.apache.beam.sdk.schemas.io.pubsub;


import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PubsubSchemaCapableIOProviderTest {
    @Test
    public void testProviderTypePubsub() {
        PubsubSchemaCapableIOProvider provider = new PubsubSchemaCapableIOProvider();
        assertEquals("pubsub", provider.identifier());
    }

    @Test
    public void testCreateProvider() {
        PubsubSchemaCapableIOProvider provider = new PubsubSchemaCapableIOProvider();
        System.out.println(provider.configurationSchema());
        assertEquals("pubsub", provider.identifier());
    }


}
