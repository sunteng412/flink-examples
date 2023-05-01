package com.qingxuan.flink.example.custom.source;
 

import cn.hutool.http.HttpRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class HttpSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
 
    private String url;
    private String body;
    private String type;
    private static final String HTTP_GET = "GET";
    private DeserializationSchema<RowData> deserializer;
 
    public HttpSourceFunction() {
 
    }
 
    public HttpSourceFunction(String url, String body, String type, DeserializationSchema<RowData> deserializer) {
        this.url = url;
        this.body = body;
        this.type = type;
        this.deserializer = deserializer;
    }
 
    @Override
    public void open(Configuration parameters) throws Exception {
        deserializer.open(new DeserializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return getRuntimeContext().getMetricGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return SimpleUserCodeClassLoader.create(Thread.currentThread().getContextClassLoader());
            }
        });
    }
 
    @Override
    public void close() throws IOException {
 
    }
 
    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }
 
    public static HttpSourceFunction.Builder builder() {
        return new HttpSourceFunction.Builder();
    }
 

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {

        while (true){
            try {

                String execute;
                if (StringUtils.isNotBlank(type) && HTTP_GET.equalsIgnoreCase(type)) {
                    execute =  HttpRequest.get(url).header("Content-type", "application/json")
                            .timeout(5000).execute().body();

                } else {
                    execute =  HttpRequest.post(url).body(body).header("Content-type", "application/json")
                            .timeout(5000).execute().body();

                }

                sourceContext.collect(deserializer.deserialize(execute.getBytes(StandardCharsets.UTF_8)));
            } catch (Throwable t) {
                t.printStackTrace(); // print and continue
            }
            TimeUnit.SECONDS.sleep(1);
        }

    }
 
    @Override
    public void cancel() {
    }
 
    public static class Builder {
        private String url;
        private String body;
        private String type;
        private DeserializationSchema<RowData> deserializer;
 
        public Builder () {
 
        }
 
        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }
 
        public Builder setBody(String body) {
            this.body = body;
            return this;
        }
 
        public Builder setType(String type) {
            this.type = type;
            return this;
        }
 
        public Builder setDeserializer(DeserializationSchema<RowData> deserializer) {
            this.deserializer = deserializer;
            return this;
        }
 
        public HttpSourceFunction build() {
            if (StringUtils.isBlank(url)  || StringUtils.isBlank(type)) {
                throw new IllegalArgumentException("params has null");
            }
            return new HttpSourceFunction(this.url, this.body, this.type, this.deserializer);
        }
 
    }
}