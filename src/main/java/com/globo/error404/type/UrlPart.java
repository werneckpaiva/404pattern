package com.globo.error404.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class UrlPart implements Writable, WritableComparable<UrlPart>, Cloneable{

    private Integer position;
    private String part;

    public UrlPart() {
        super();
        position = 0;
        part = "";
    }

    public UrlPart(Integer position, String part) {
        super();
        this.position = position;
        this.part = part;
    }

    public Integer getPosition() {
        return position;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public String getPart() {
        return part;
    }

    public void setPart(String part) {
        this.part = part;
    }

    public int compareTo(UrlPart urlPart) {
        Integer result = position.compareTo(urlPart.getPosition());
        if (result == 0){
            result = part.compareTo(urlPart.getPart());
        }
        return result;
    }

    public void readFields(DataInput input) throws IOException {
        position = input.readInt();
        part = input.readUTF();
    }

    public void write(DataOutput output) throws IOException {
        output.writeInt(position);
        output.writeUTF(part);
    }

    @Override
    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append("[").append(position).append("] ");
        sb.append(part);
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj){
        UrlPart urlRequest = (UrlPart) obj; 
        return (this.compareTo(urlRequest) == 0);
    }

    @Override
    public int hashCode(){
        return position + part.hashCode();
    }

    @Override
    public Object clone() {
       return new UrlPart(position, part);
    }
}
