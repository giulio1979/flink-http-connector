package com.getindata.connectors.http.internal.table.lookup.querycreators;

import com.getindata.connectors.http.LookupQueryCreator;
import com.getindata.connectors.http.LookupQueryCreatorFactory;
import com.getindata.connectors.http.internal.table.lookup.LookupRow;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Set;


/**
 * Factory for creating {@link GenericGetQueryCreator}.
 */
public class CustomGetQueryCreatorFactory implements LookupQueryCreatorFactory {

    public static final String IDENTIFIER = "custom-get-query";

    @Override
    public LookupQueryCreator createLookupQueryCreator(
            ReadableConfig readableConfig,
            LookupRow lookupRow,
            DynamicTableFactory.Context dynamicTableFactoryContext) {
        return new CustomGetQueryCreator(readableConfig, lookupRow);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }
}
