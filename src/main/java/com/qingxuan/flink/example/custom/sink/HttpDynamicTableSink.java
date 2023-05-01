package com.qingxuan.flink.example.custom.sink;

import com.qingxuan.flink.example.custom.source.HttpSourceInfo;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;

/**
 * 自定义httpSink
 *
 * @author : longchuan
 * @version :
 * @date : 2023/3/10 3:31 下午
 * @description:
 */
public class HttpDynamicTableSink implements DynamicTableSink {

    private HttpSourceInfo httpSourceInfo;
    private EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private ResolvedSchema tableSchema;


    public HttpDynamicTableSink(HttpSourceInfo httpSourceInfo,
                                EncodingFormat<SerializationSchema<RowData>> encodingFormat,
                                ResolvedSchema resolvedSchema) {
        this.httpSourceInfo = httpSourceInfo;
        this.tableSchema = resolvedSchema;
        this.encodingFormat = encodingFormat;

    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return changelogMode;
    }

    /**
     * 具体运行的地方，真正开始调用用户自己定义的 streaming sink ，
     * 建立 sql 与 streaming 的联系
     * @param
     * @return
     * @description:
     */
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(
                HttpSinkFunction.builder()
                        .setUrl(httpSourceInfo.getUrl())
                        .setDeserializer(encodingFormat.createRuntimeEncoder(context,tableSchema.toPhysicalRowDataType()))
                        .setBody(httpSourceInfo.getBody())
                        .setType(httpSourceInfo.getType())
                        .setFields(tableSchema.getColumnNames())
                        .setDataTypes(tableSchema.toPhysicalRowDataType().getChildren()).build()
                );
    }

    /**
     * sink 可以不用实现，主要用来 source 的谓词下推
     * @param
     * @return
     * @description:
     */
    @Override
    public DynamicTableSink copy() {
        return new HttpDynamicTableSink(httpSourceInfo, encodingFormat, tableSchema);
    }


    @Override
    public String asSummaryString() {
        return "custom_http_source";
    }
}
