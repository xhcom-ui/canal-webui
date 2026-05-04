package com.openclaw.canalweb.dto;

public record LoginResponse(String tokenName, String tokenValue, UserInfo user) {
}
