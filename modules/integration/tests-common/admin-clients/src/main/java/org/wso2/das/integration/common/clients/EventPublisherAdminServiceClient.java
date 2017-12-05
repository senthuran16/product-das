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
import org.wso2.carbon.event.publisher.stub.EventPublisherAdminServiceStub;
import org.wso2.carbon.event.publisher.stub.types.BasicOutputAdapterPropertyDto;
import org.wso2.carbon.event.publisher.stub.types.EventPublisherConfigurationInfoDto;

import java.rmi.RemoteException;

public class EventPublisherAdminServiceClient {
    private static final Log log = LogFactory.getLog(EventPublisherAdminServiceClient.class);
    private final String serviceName = "EventPublisherAdminService";
    private EventPublisherAdminServiceStub eventPublisherAdminServiceStub;
    private String endPoint;

    public EventPublisherAdminServiceClient(String backEndUrl, String sessionCookie) throws
                                                                                     AxisFault {
        this.endPoint = backEndUrl + serviceName;
        eventPublisherAdminServiceStub = new EventPublisherAdminServiceStub(endPoint);
        AuthenticateStubUtil.authenticateStub(sessionCookie, eventPublisherAdminServiceStub);

    }

    public ServiceClient _getServiceClient() {
        return eventPublisherAdminServiceStub._getServiceClient();
    }

    public int getActiveEventPublisherCount()
            throws RemoteException {
        try {
            EventPublisherConfigurationInfoDto[] configs = eventPublisherAdminServiceStub.getAllActiveEventPublisherConfigurations();
            if (configs == null) {
                return 0;
            } else {
                return configs.length;
            }
        } catch (RemoteException e) {
            throw new RemoteException("RemoteException", e);
        }
    }

    public void addEventPublisherConfiguration(String eventPublisherConfigXml)
            throws RemoteException {
        try {
            eventPublisherAdminServiceStub.deployEventPublisherConfiguration(eventPublisherConfigXml);
        } catch (RemoteException e) {
            log.error("RemoteException", e);
            throw new RemoteException();
        }
    }

    public void removeActiveEventPublisherConfiguration(String eventPublisherName)
            throws RemoteException {
        try {
            eventPublisherAdminServiceStub.undeployActiveEventPublisherConfiguration(eventPublisherName);
        } catch (RemoteException e) {
            log.error("RemoteException", e);
            throw new RemoteException();
        }
    }

    public void removeInactiveEventPublisherConfiguration(String fileName)
            throws RemoteException {
        try {
            eventPublisherAdminServiceStub.undeployInactiveEventPublisherConfiguration(fileName);
        } catch (RemoteException e) {
            log.error("RemoteException", e);
            throw new RemoteException();
        }
    }

    public void testConnection(String eventPublisherName, String eventPublisherType,
                               BasicOutputAdapterPropertyDto[] outputPropertyConfiguration, String messageFormat)
            throws RemoteException {
        eventPublisherAdminServiceStub.testPublisherConnection(eventPublisherName, eventPublisherType,
                outputPropertyConfiguration, messageFormat);
    }
}
