/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs.storage;

import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.GcsTestUtils.allocator;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class StorageMetadataTest
{
    private StorageMetadata storageMetadata;

    private List<Blob> blobList;
    @Mock
    private Page<Blob> blobPage;
    @Mock
    GoogleCredentials credentials;
    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;
    @Mock
    Storage storage;
    @Mock
    Blob blob;

    static {
        Mockito.mockStatic(StorageOptions.class);
        Mockito.mockStatic(ServiceAccountCredentials.class);
        Mockito.mockStatic(GoogleCredentials.class);
    }

    @Before
    public void setUp() throws Exception
    {
        StorageOptions.Builder optionBuilder = mock(StorageOptions.Builder.class);
        Mockito.when(StorageOptions.newBuilder()).thenReturn(optionBuilder);
        StorageOptions mockedOptions = mock(StorageOptions.class);
        Mockito.when(optionBuilder.setCredentials(ArgumentMatchers.any())).thenReturn(optionBuilder);
        Mockito.when(optionBuilder.build()).thenReturn(mockedOptions);
        Mockito.when(mockedOptions.getService()).thenReturn(storage);
        blobList = ImmutableList.of(blob);

        String gcsJson = "{\"gcs_credential_keys\":\"{\\\"type\\\": \\\"service_account\\\",\\\"project_id\\\": \\\"afq\\\",\\\"private_key_id\\\": \\\"6d559a25a53c666e6123456\\\",\\\"private_key\\\": \\\"-----BEGIN PRIVATE KEY-----\\\\n3cBBBa/2Bouf76nUmf91Ptg=\\\\n-----END PRIVATE KEY-----\\\\n\\\",\\\"client_email\\\": \\\"afq.iam.gserviceaccount.com\\\",\\\"client_id\\\": \\\"10947997337471234567\\\",\\\"auth_uri\\\": \\\"https://accounts.google.com/o/oauth2/auth\\\",\\\"token_uri\\\": \\\"https://oauth2.googleapis.com/token\\\",\\\"auth_provider_x509_cert_url\\\": \\\"https://www.googleapis.com/oauth2/v1/certs\\\",\\\"client_x509_cert_url\\\": \\\"https://www.googleapis.com/robot/v1/metadata/x509/afq.iam.gserviceaccount.com\\\"}\",\"gcs_HMAC_key\":\"GOOG1EGNWCPMWNY5IOMRELOVM22ZQEBEVDS7NX\",\"gcs_HMAC_secret\":\"haK0skzuPrUljknEsfcRJCYR\"}";

        Mockito.when(ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        MockitoAnnotations.initMocks(this);
        Mockito.when(GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        Mockito.when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);
        storageMetadata = new StorageMetadata(gcsJson);
    }

    private void storageMock() throws IllegalAccessException
    {
        FieldUtils.writeField(storageMetadata, "storage", storage, true);
        Mockito.when(storage.list(any(),any())).thenReturn(blobPage);
        blobList = ImmutableList.of(blob);
        Mockito.when(blob.getName()).thenReturn("birthday.parquet");
        Mockito.when(blob.getSize()).thenReturn(10L);
        Mockito.when(blobPage.iterateAll()).thenReturn(blobList);
    }

    @Test
    public void testBuildTableSchema() throws Exception
    {
        Table table = new Table();
        table.setName("birthday");
        table.setDatabaseName("default");
        table.setParameters(ImmutableMap.of("classification", "parquet"));
        table.setStorageDescriptor(new StorageDescriptor()
                .withLocation("gs://mydatalake1test/birthday/"));
        table.setCatalogId("catalog");
        storageMetadata = mock(StorageMetadata.class);
        storageMock();
        when(storageMetadata.buildTableSchema(any(), any())).thenCallRealMethod();
        when(storageMetadata.getAnyFilenameInPath(any(),any())).thenCallRealMethod();
        Field field = new Field("year", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("dataFormat", "parquet");
        Schema schema = new Schema(asList(field), metadataSchema);
        when(storageMetadata.getFileSchema(any(),any(),any(), any())).thenReturn(schema);
        Schema outSchema = storageMetadata.buildTableSchema(table, allocator);
        assertNotNull(outSchema);
    }
}
