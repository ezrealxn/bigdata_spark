package com.ly.gmll.interceptor;

import com.alibaba.fastjson.JSON;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class EtlLogInterceptor implements Interceptor{


    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        try {
            JSON.parseObject(body);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        Iterator<Event> iterator = events.iterator();
        while (iterator.hasNext()){

            Event event = iterator.next();
            Event result = intercept(event);
            if(result == null){
                iterator.remove();
            }
        }
        return events;
    }

    @Override
    public void close() {

    }


    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new EtlLogInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
