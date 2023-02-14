package com.ly.gmll.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TimestampInterceptor implements Interceptor{
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //取出event的body
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        //将字符串解析成json对象
        JSONObject jsonObject = JSON.parseObject(body);
        //取出消息的时间戳
        String ts = jsonObject.getString("ts");
        //将时间戳设置成event的timestamp
        event.getHeaders().put("timestamp",ts);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
