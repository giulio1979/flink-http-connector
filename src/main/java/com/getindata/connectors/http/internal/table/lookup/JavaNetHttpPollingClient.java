package com.getindata.connectors.http.internal.table.lookup;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.StringUtils;

import com.getindata.connectors.http.LookupArg;
import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.HeaderPreprocessor;
import com.getindata.connectors.http.internal.PollingClient;
import com.getindata.connectors.http.internal.config.HttpConnectorConfigConstants;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker;
import com.getindata.connectors.http.internal.status.ComposeHttpStatusCodeChecker.ComposeHttpStatusCodeCheckerConfig;
import com.getindata.connectors.http.internal.status.HttpStatusCodeChecker;
import com.getindata.connectors.http.internal.utils.HttpHeaderUtils;
import com.getindata.connectors.http.internal.utils.uri.URIBuilder;

/**
 * An implementation of {@link PollingClient} that uses Java 11's {@link HttpClient}.
 * This implementation supports HTTP traffic only.
 */
@Slf4j
public class JavaNetHttpPollingClient implements PollingClient<RowData> {

    private final HttpClient httpClient;

    private final HttpStatusCodeChecker statusCodeChecker;

    private final DeserializationSchema<RowData> runtimeDecoder;

    private final HttpLookupConfig options;

    private final LookupQueryCreator lookupQueryCreator;

    private final String[] headersAndValues;

    public JavaNetHttpPollingClient(
            HttpClient httpClient,
            DeserializationSchema<RowData> runtimeDecoder,
            HttpLookupConfig options,
            LookupQueryCreator lookupQueryCreator,
            HeaderPreprocessor headerPreprocessor) {

        this.httpClient = httpClient;
        this.runtimeDecoder = runtimeDecoder;
        this.options = options;
        this.lookupQueryCreator = lookupQueryCreator;

        var headerMap = HttpHeaderUtils
            .prepareHeaderMap(
                HttpConnectorConfigConstants.LOOKUP_SOURCE_HEADER_PREFIX,
                options.getProperties(),
                headerPreprocessor
            );
        this.headersAndValues = HttpHeaderUtils.toHeaderAndValueArray(headerMap);

        // TODO Inject this via constructor when implementing a response processor.
        //  Processor will be injected and it will wrap statusChecker implementation.
        ComposeHttpStatusCodeCheckerConfig checkerConfig =
            ComposeHttpStatusCodeCheckerConfig.builder()
                .properties(options.getProperties())
                .whiteListPrefix(
                    HttpConnectorConfigConstants.HTTP_ERROR_SOURCE_LOOKUP_CODE_WHITE_LIST
                )
                .errorCodePrefix(HttpConnectorConfigConstants.HTTP_ERROR_SOURCE_LOOKUP_CODES_LIST)
                .build();

        this.statusCodeChecker = new ComposeHttpStatusCodeChecker(checkerConfig);
    }

    @Override
    public Optional<RowData> pull(List<LookupArg> lookupArgs) {
        try {
            return queryAndProcess(lookupArgs);
        } catch (Exception e) {
            log.error("Exception during HTTP request.", e);
            return Optional.empty();
        }
    }

    // TODO Add Retry Policy And configure TimeOut from properties
    private Optional<RowData> queryAndProcess(List<LookupArg> params) throws Exception {

        HttpRequest request = buildHttpRequest(params);
        HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
        return processHttpResponse(response, request);
    }

    private HttpRequest buildHttpRequest(List<LookupArg> params) throws URISyntaxException {
        var lookupQuery = lookupQueryCreator.createLookupQuery(params);
        URI uri = new URIBuilder(options.getUrl() + "?" + lookupQuery).build();

        Builder requestBuilder = HttpRequest.newBuilder()
            .uri(uri).GET()
            .timeout(Duration.ofMinutes(2));

        if (headersAndValues.length != 0) {
            requestBuilder.headers(headersAndValues);
        }

        return requestBuilder.build();
    }

    private Optional<RowData> processHttpResponse(
            HttpResponse<String> response,
            HttpRequest request) throws IOException {

        if (response == null) {
            log.warn("Null Http response for request " + request.uri().toString());
            return Optional.empty();
        }

        String body = response.body();
        int statusCode = response.statusCode();

        log.debug("Received {} status code for RestTableSource Request", statusCode);
        if (notErrorCodeAndNotEmptyBody(body, statusCode)) {
            log.trace("Server response body" + body);
            return Optional.ofNullable(runtimeDecoder.deserialize(body.getBytes()));
        } else {
            log.warn(
                String.format("Returned Http status code was invalid or returned body was empty. "
                + "Status Code [%s], "
                + "response body [%s]", statusCode, body)
            );

            return Optional.empty();
        }
    }

    private boolean notErrorCodeAndNotEmptyBody(String body, int statusCode) {
        return !(StringUtils.isNullOrWhitespaceOnly(body) || statusCodeChecker.isErrorCode(
            statusCode));
    }

    @VisibleForTesting
    String[] getHeadersAndValues() {
        return Arrays.copyOf(headersAndValues, headersAndValues.length);
    }
}
