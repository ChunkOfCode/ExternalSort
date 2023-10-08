package com.example.externalfilesort.service;

import java.io.*;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
public class QueueNode implements Comparable<QueueNode> {

    private int key;

    private String value;

    private BufferedReader reader;

    @Override
    public int compareTo(QueueNode other) {
        return Integer.compare(this.key, other.key);
    }

}
