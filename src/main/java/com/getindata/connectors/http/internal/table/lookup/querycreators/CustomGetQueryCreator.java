package com.getindata.connectors.http.internal.table.lookup.querycreators;

import com.getindata.connectors.http.LookupArg;
import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.internal.table.lookup.HttpLookupConnectorOptions;
import com.getindata.connectors.http.internal.table.lookup.LookupQueryInfo;
import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import com.getindata.connectors.http.internal.utils.uri.NameValuePair;
import com.getindata.connectors.http.internal.utils.uri.URLEncodedUtils;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link LookupQueryCreator} that builds an "ordinary" GET query, i.e. adds
 * <code>joinColumn1=value1&amp;joinColumn2=value2&amp;...</code> to the URI of the endpoint.
 */
public class CustomGetQueryCreator implements LookupQueryCreator {

    private final LookupRow lookupRow;
    private final ReadableConfig readableConfig;

    public CustomGetQueryCreator(ReadableConfig readableConfig,
                                 LookupRow lookupRow) {
        this.lookupRow = lookupRow;
        this.readableConfig = readableConfig;
    }

    @Override
    public LookupQueryInfo createLookupQuery(RowData lookupDataRow) {

        Collection<LookupArg> lookupArgs = lookupRow.convertToLookupArgs(lookupDataRow);
        Map<String, String> pathBasedUrlParams = new HashMap<>();

        for(LookupArg a : lookupArgs)
            if(readableConfig.get(HttpLookupConnectorOptions.URL).indexOf("{" + a.getArgName() + "}") > 0)
                pathBasedUrlParams.put(a.getArgName(), a.getArgValue());

        String lookupQuery =
            URLEncodedUtils.format(
                lookupArgs.stream()
                        .map(arg -> new NameValuePair(arg.getArgName(), arg.getArgValue()))
                        .collect(Collectors.toList()),
                StandardCharsets.UTF_8);

        return new LookupQueryInfo(lookupQuery,null,pathBasedUrlParams);
    }
}
