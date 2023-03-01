/*-
 * #%L
 * athena-cloudera-impala
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.Assert;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JDBCUtil.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*","org.w3c.*","javax.net.ssl.*","sun.security.*","jdk.internal.reflect.*"})
public class ImpalaCompositeHandlerTest
{
    @BeforeClass
    public static void dataSetUP() {
        System.setProperty("aws.region", "us-west-2");
    }
    @Test
    public void ImpalaCompositeHandlerTest(){
        Exception ex = null;
        try {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog1", ImpalaConstants.IMPALA_NAME,
                "impala://jdbc:impala://54.89.6.2:10000/authena;AuthMech=3;${testSecret}","testSecret");
        PowerMockito.mockStatic(JDBCUtil.class);
        JDBCUtil tested = PowerMockito.mock(JDBCUtil.class);
        PowerMockito.when(tested.getSingleDatabaseConfigFromEnv(ImpalaConstants.IMPALA_NAME, System.getenv())).thenReturn(databaseConnectionConfig);
        new ImpalaCompositeHandler();
        }catch(Exception e) {
            ex =e;
        }
        Assert.assertEquals(null, ex);
    }

}
