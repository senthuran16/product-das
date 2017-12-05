/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.das.integration.common.clients;


import org.apache.axis2.AxisFault;
import org.apache.axis2.client.ServiceClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.event.stream.stub.EventStreamAdminServiceStub;
import org.wso2.carbon.event.stream.stub.types.EventStreamInfoDto;

import java.rmi.RemoteException;

public class EventStreamManagerAdminServiceClient {
    private static final Log log = LogFactory.getLog(EventStreamManagerAdminServiceClient.class);
    private final String serviceName = "EventStreamAdminService";
    private EventStreamAdminServiceStub eventStreamAdminServiceStub;
    private String endPoint;

    public EventStreamManagerAdminServiceClient(String backEndUrl, String sessionCookie) throws
                                                                                         AxisFault {
        this.endPoint = backEndUrl + serviceName;
        eventStreamAdminServiceStub = new EventStreamAdminServiceStub(endPoint);
        AuthenticateStubUtil.authenticateStub(sessionCookie, eventStreamAdminServiceStub);

    }

    public ServiceClient _getServiceClient() {
        return eventStreamAdminServiceStub._getServiceClient();
    }

    public int getEventStreamCount()
            throws RemoteException {
        try {
            EventStreamInfoDto[] streamInfoDtos = eventStreamAdminServiceStub.getAllEventStreamDefinitionDto();
            if (streamInfoDtos == null) {
                return 0;
            } else {
                return streamInfoDtos.length;
            }
        } catch (RemoteException e) {
            throw new RemoteException("RemoteException", e);
        }
    }

    public void addEventStreamAsString(String eventStreamDefinition)
            throws RemoteException {
        try {
            eventStreamAdminServiceStub.addEventStreamDefinitionAsString(eventStreamDefinition);
        } catch (RemoteException e) {
            log.error("RemoteException", e);
            throw new RemoteException();
        }
    }

    public void removeEventStream(String eventStreamName, String eventStreamVersion)
            throws RemoteException {
        try {
            eventStreamAdminServiceStub.removeEventStreamDefinition(eventStreamName, eventStreamVersion);
        } catch (RemoteException e) {
            log.error("RemoteException", e);
            throw new RemoteException();
        }
    }
}
