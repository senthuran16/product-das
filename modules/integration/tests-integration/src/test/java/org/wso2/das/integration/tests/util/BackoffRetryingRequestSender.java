package org.wso2.das.integration.tests.util;

import org.apache.http.annotation.NotThreadSafe;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.wso2.carbon.automation.test.utils.http.client.HttpRequestUtil;
import org.wso2.carbon.automation.test.utils.http.client.HttpResponse;
import org.wso2.das.integration.common.utils.TestConstants;
import org.wso2.das.integration.common.utils.Utils;

import java.net.URI;
import java.net.URL;
import java.util.Map;

/**
 * Contains methods to do HTTP Requests with backoff retrying and return responses.
 */
public class BackoffRetryingRequestSender {
    public static HttpResponse doHttpPost(URL endpoint, String postBody, Map<String, String> headers) throws Exception {
        HttpResponse response = null;
        BackoffRetryCounter backoffRetryCounter = new BackoffRetryCounter();
        boolean codeOK = false;
        while (!codeOK) {
            response = HttpRequestUtil.doPost(endpoint, postBody, headers);
            codeOK = (response.getResponseCode() == 200) && !response.getData().contains("[]");
            if (!codeOK) {
                Thread.sleep(backoffRetryCounter.getTimeInterval());
                backoffRetryCounter.increment();
            }
            if (backoffRetryCounter.shouldStop()) {
                break;
            }
        }
        return response;
    }

    public static HttpResponse doHttpGet(String endpoint, Map<String, String> headers) throws Exception {
        HttpResponse response = null;
        BackoffRetryCounter backoffRetryCounter = new BackoffRetryCounter();
        boolean codeOK = false;
        while (!codeOK) {
            response = Utils.doGet(endpoint, headers);
            codeOK = (response.getResponseCode() == 200) && !response.getData().contains("[]");
            if (!codeOK) {
                Thread.sleep(backoffRetryCounter.getTimeInterval());
                backoffRetryCounter.increment();
            }
            if (backoffRetryCounter.shouldStop()) {
                break;
            }
        }
        return response;
    }

    public static org.apache.http.HttpResponse doHttpDelete(String uri) throws Exception {
        HttpClient httpClient = new DefaultHttpClient();
        org.apache.http.HttpResponse response = null;

        HttpDeleteWithBody httpDelete = new HttpDeleteWithBody(uri);
        httpDelete.setHeader("Authorization", TestConstants.BASE64_ADMIN_ADMIN);

        BackoffRetryCounter backoffRetryCounter = new BackoffRetryCounter();
        boolean codeOK = false;
        while (!codeOK) {
            response = httpClient.execute(httpDelete);
            codeOK = (response.getStatusLine().getStatusCode() == 200);
            if (!codeOK) {
                Thread.sleep(backoffRetryCounter.getTimeInterval());
                backoffRetryCounter.increment();
            }
            if (backoffRetryCounter.shouldStop()) {
                break;
            }
        }
        return response;
    }
}

@NotThreadSafe
class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase {
    public static final String METHOD_NAME = "DELETE";
    public String getMethod() { return METHOD_NAME; }

    public HttpDeleteWithBody(final String uri) {
        super();
        setURI(URI.create(uri));
    }
}
