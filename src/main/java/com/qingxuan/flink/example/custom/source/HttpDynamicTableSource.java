package com.qingxuan.flink.example.custom.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * changelog 流（支持有界或无界），在 changelog 流结束前，所有的改变都会被源源不断地消费，由 ScanTableSource 接口表示。
 * 处于一直变换或数据量很大的外部表，其中的数据一般不会被全量读取，除非是在查询某个值时，由 LookupTableSource 接口表示。
 * @author     : longchuan
 * @date       : 2023/3/8 5:30 下午
 * @description:
 * @version    :
 */
public class HttpDynamicTableSource implements ScanTableSource {
 
    private HttpSourceInfo httpSourceInfo;
 
    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private DataType producedDataType;
 
    public HttpDynamicTableSource() {
 
    }
 
    public HttpDynamicTableSource(HttpSourceInfo httpSourceInfo,
                                  DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                  DataType producedDataType) {
        this.httpSourceInfo = httpSourceInfo;
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
    }
 
    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }
 
    /**
     * 最主要的方法，就是实现这个接口方法，在方法里面返回具体的实现逻辑类的构造
     * @param
     * @return
     * @description:
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
                scanContext,
                producedDataType);
        HttpSourceFunction.Builder builder = HttpSourceFunction.builder()
                .setUrl(httpSourceInfo.getUrl()).setBody(httpSourceInfo.getBody())
                .setType(httpSourceInfo.getType()).setDeserializer(deserializer);
 
        return SourceFunctionProvider.of(builder.build(), true);
    }
 
    @Override
    public DynamicTableSource copy() {
        return new HttpDynamicTableSource(this.httpSourceInfo, this.decodingFormat, this.producedDataType);
    }
 
    @Override
    public String asSummaryString() {
        return "custom_http_source";
    }
}