/*-
 * #%L
 * athena-federation-sdk
 * %%
 * Copyright (C) 2023 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.amazonaws.athena.connector.lambda.data;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.*;

public class SupportedTypesTest
{

    // Copied from: athena-gcs/src/main/java/com/amazonaws/athena/connectors/gcs/storage/StorageMetadata.java
    // Used to attempt to reproduce the error being seen.
    private static ArrowType getCompatibleFieldType(ArrowType arrowType)
    {
        switch (arrowType.getTypeID()) {
            case Time: {
                return Types.MinorType.DATEMILLI.getType();
            }
            case Timestamp: {
                return new ArrowType.Timestamp(
                    org.apache.arrow.vector.types.TimeUnit.MILLISECOND,
                    ((ArrowType.Timestamp) arrowType).getTimezone());
            }
            // NOTE: Not sure that both of these should go to Utf8,
            // but just keeping it in-line with how it was before.
            case FixedSizeBinary:
            case LargeBinary:
                return ArrowType.Utf8.INSTANCE;
        }
        return arrowType;
    }

    @Test
    public void testTimestampMilliCompatible() {
        // Basic test:
        assertTrue(SupportedTypes.isSupported(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)));

        // Now test the scenario in athena-gcs/src/main/java/com/amazonaws/athena/connectors/gcs/storage/StorageMetadata.java
        Field asdfField = new Field(
            "asdf",
            new FieldType(true, new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, null), null),
            ImmutableList.of());

        try {
            SupportedTypes.assertSupported(asdfField);
            fail("Expected an exception to be thrown");
        }
        catch (RuntimeException ex) {
            // Ignore, this is expected
        }

        Field updatedField = ArrowSchemaUtils.remapArrowTypesWithinField(asdfField, SupportedTypesTest::getCompatibleFieldType);

        // Expect no assertion to be thrown.
        // This assertion was what we were seeing being thrown during your testing.
        SupportedTypes.assertSupported(updatedField);
    }
}
