package com.utils;

public class Record {

    private String id ;
    private String name ;
    private String surname ;
    private String city ;

    public Record(String id, String name, String surname, String city) {
        this.id = id;
        this.name = name;
        this.surname = surname;
        this.city = city;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
