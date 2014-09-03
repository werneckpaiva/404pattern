package com.globo.error404.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class UrlRequest implements Writable, WritableComparable<UrlRequest>, Cloneable{


    private String request;
    private String logEntry;
    private Integer code;

    public UrlRequest() {

    }

    public UrlRequest(String request, String logEntry, Integer code) {
        super();
        this.request = request;
        this.logEntry = logEntry;
        this.code = code;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getLogEntry() {
        return logEntry;
    }

    public void setLogEntry(String logEntry) {
        this.logEntry = logEntry;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public int compareTo(UrlRequest urlRequest) {
        return request.compareTo(urlRequest.getRequest());
    }

    public void readFields(DataInput input) throws IOException {
        request = input.readUTF();
        code = input.readInt();
        logEntry = input.readUTF();
    }

    public void write(DataOutput output) throws IOException {
        output.writeUTF(request);
        output.writeInt(code);
        output.writeUTF(logEntry);
    }

    public static UrlRequest getInstanceFromLogEntry(String entry){
        String[] parts = entry.split(" "); // TODO: replace with a log format
        
        UrlRequest urlRequest = new UrlRequest();
        urlRequest.setLogEntry(entry);
        try{
            urlRequest.setCode(Integer.parseInt(parts[10]));
        } catch (NumberFormatException e) { }
        urlRequest.setRequest(parts[7]);
        return urlRequest;
    }

    public String[] getUrlParts(){
        return request.split("/");
    }
    
    @Override
    public String toString(){
        return request;
    }

    @Override
    public boolean equals(Object obj){
        UrlRequest urlRequest = (UrlRequest) obj; 
        return (this.compareTo(urlRequest) == 0);
    }

    @Override
    public int hashCode(){
        return request.hashCode();
    }

    public String getNormalizedRequest(){
        String normalizedUrl = request.replaceAll("/[0-9]+[/\\.]", "");
        return normalizedUrl;
    }

    
    @Override
    public Object clone() {
       return new UrlRequest(request, logEntry, code);
    }

}
