package com.bwarelabs.common;

public record BigtableCell(String key, byte[] value) {
}
