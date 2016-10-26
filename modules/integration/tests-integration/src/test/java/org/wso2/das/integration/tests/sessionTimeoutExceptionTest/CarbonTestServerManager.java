/*
*  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.das.integration.tests.sessionTimeoutExceptionTest;

import java.io.IOException;
import java.util.Map;

import org.wso2.carbon.automation.extensions.servers.carbonserver.TestServerManager;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.exceptions.AutomationFrameworkException;
import javax.xml.xpath.XPathExpressionException;

public class CarbonTestServerManager extends TestServerManager {

    public CarbonTestServerManager(AutomationContext context) throws XPathExpressionException {
        super(context);
    }
    public CarbonTestServerManager(AutomationContext context, String carbonZip, Map<String, String> startupParameterMap)
            throws XPathExpressionException {
        super(context, carbonZip, startupParameterMap);
    }

    public CarbonTestServerManager(AutomationContext context, int portOffset) throws XPathExpressionException {
        super(context, portOffset);
    }

    public String startServer() throws AutomationFrameworkException, IOException, XPathExpressionException {
        carbonHome = super.startServer();
        return carbonHome;
    }

    public void stopServer() throws AutomationFrameworkException {
        super.stopServer();
    }

    public String getCarbonHome() {
        return carbonHome;
    }
}