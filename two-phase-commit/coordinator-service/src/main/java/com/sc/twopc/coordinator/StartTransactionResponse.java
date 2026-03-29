package com.sc.twopc.coordinator;

public record StartTransactionResponse(
		String transactionId,
		String orderStatus,
		String paymentStatus,
		String overallResult) {
}
