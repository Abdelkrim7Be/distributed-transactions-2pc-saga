package com.sc.twopc.coordinator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ParticipantTransactionResponse(String status, String message) {
}
