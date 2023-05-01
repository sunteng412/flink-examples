package com.qingxuan.flink.example.custom;
 
import com.qingxuan.flink.example.custom.sink.HttpDynamicTableSink;
import com.qingxuan.flink.example.custom.source.HttpDynamicTableSource;
import com.qingxuan.flink.example.custom.source.HttpSourceInfo;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
 
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * 这里是同时实现了source和sink，也可以单独去实现，写在一起可以减少很多重复代码
 * @author     : longchuan
 * @date       : 2023/3/7 4:09 下午
 * @description:
 * @version    :
 */
public class HttpDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
 
/**
    首先定义一些数据源的参数信息，就是连接器的所有参数都需要先定义，这样才能在SQL里面去使用
*/
    public static final String IDENTIFIER = "http";
    public static final ConfigOption<String> URL = ConfigOptions.key("url")
            .stringType().noDefaultValue().withDescription("the jdbc database url.");
    public static final ConfigOption<String> HEADERS = ConfigOptions.key("headers")
            .stringType().noDefaultValue().withDescription("the http header.");
    private static final ConfigOption<String> BODY = ConfigOptions.key("body")
            .stringType().noDefaultValue().withDescription("the http body params.");
    private static final ConfigOption<String> TYPE = ConfigOptions.key("type")
            .stringType().noDefaultValue().withDescription("the http type.");
    private static final ConfigOption<String> FORMAT = ConfigOptions.key("format")
            .stringType().noDefaultValue().withDescription("the http type.");
 
    public HttpDynamicTableFactory() {
    }
 
    /**
     * 构造source类型的数据源对象
     * @param
     * @return
     * @description:
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig config = helper.getOptions();
        helper.validate();
        this.validateConfigOptions(config);
        HttpSourceInfo httpSourceInfo = this.getHttpSource(config);
        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);

        final DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new HttpDynamicTableSource(httpSourceInfo, decodingFormat, producedDataType);
    }
 
    /**
     * 构造sink类型的数据源对象
     * @param
     * @return
     * @description:
     */
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        this.validateConfigOptions(config);

        HttpSourceInfo httpSourceInfo = this.getHttpSource(config);
        // discover a suitable encoding format
        final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);
 
        // derive the produced data type (excluding computed columns) from the catalog table
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        return new HttpDynamicTableSink(httpSourceInfo, encodingFormat, resolvedSchema);
    }
 
    /**
     * 获取自定义的HTTP连接器的参数对象，主要用来存HTTP链接的一些参数，后面用来构造HTTP请求使用
     * @param
     * @return
     * @description:
     */
    private HttpSourceInfo getHttpSource(ReadableConfig readableConfig) {
        HttpSourceInfo httpSourceInfo = new HttpSourceInfo();
        httpSourceInfo.setBody(readableConfig.get(BODY));
        httpSourceInfo.setHeaders(readableConfig.get(HEADERS));
        httpSourceInfo.setType(readableConfig.get(TYPE));
        httpSourceInfo.setUrl(readableConfig.get(URL));
       return httpSourceInfo;
    }
 
    /**
     * 返回数据源的type字符串，标识这是一个什么数据源
     * @param
     * @return
     * @description:
     */
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
 
    /**
     * 必填参数需要在这个方法里面去添加
     * @param
     * @return
     * @description:
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet();
        requiredOptions.add(URL);
        return requiredOptions;
    }

    /**
     * 非必填参数需要在这个方法里面去添加
     * @param
     * @return
     * @description:
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet();
        optionalOptions.add(TYPE);
        optionalOptions.add(HEADERS);
        optionalOptions.add(BODY);
        optionalOptions.add(FORMAT);
        return optionalOptions;
    }
 
// 参数校验，根据实际情况去实现需要校验哪些参数，比如有些参数有格式校验可以在这里实现，没有可以不实现
    private void validateConfigOptions(ReadableConfig config) {
        String url = config.get(URL);
        Optional<String> urlOp = Optional.of(url);
        Preconditions.checkState(urlOp.isPresent(), "Cannot handle such http url: " + url);
    }
 
}