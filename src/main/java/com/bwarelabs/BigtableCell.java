package com.bwarelabs;

public record BigtableCell(String key, byte[] value) {
}
