package com.openclaw.canalweb.common;

public record Result<T>(int code, String message, T data) {
    public static <T> Result<T> success(T data) {
        return new Result<>(0, "success", data);
    }

    public static Result<Void> success() {
        return new Result<>(0, "success", null);
    }

    public static Result<Void> failure(String message) {
        return new Result<>(500, message, null);
    }
}
