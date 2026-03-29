package com.sc.twopc.coordinator;

public class StartTransactionRequest {

	private boolean orderPrepareFail;
	private boolean paymentPrepareFail;
	private boolean orderCommitFail;
	private boolean paymentCommitFail;

	public boolean isOrderPrepareFail() {
		return orderPrepareFail;
	}

	public void setOrderPrepareFail(boolean orderPrepareFail) {
		this.orderPrepareFail = orderPrepareFail;
	}

	public boolean isPaymentPrepareFail() {
		return paymentPrepareFail;
	}

	public void setPaymentPrepareFail(boolean paymentPrepareFail) {
		this.paymentPrepareFail = paymentPrepareFail;
	}

	public boolean isOrderCommitFail() {
		return orderCommitFail;
	}

	public void setOrderCommitFail(boolean orderCommitFail) {
		this.orderCommitFail = orderCommitFail;
	}

	public boolean isPaymentCommitFail() {
		return paymentCommitFail;
	}

	public void setPaymentCommitFail(boolean paymentCommitFail) {
		this.paymentCommitFail = paymentCommitFail;
	}
}
