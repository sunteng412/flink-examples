package com.qingxuan.flink.example.custom.source;


import lombok.Data;

@Data
public class HttpSourceInfo {

    private String url;
    private String body;
    private String type;
    private String headers;

}
