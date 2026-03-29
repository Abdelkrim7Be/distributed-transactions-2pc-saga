package com.sc.twopc.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.ResponseCreator;
import org.springframework.web.client.RestTemplate;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { RestTemplateConfig.class, CoordinatorService.class })
@TestPropertySource(
		properties = {
				"coordinator.services.order=http://127.0.0.1:9/api/order",
				"coordinator.services.payment=http://127.0.0.1:9/api/payment"
		})
class CoordinatorServiceThreeScenarioTest {

	private static final String ORDER_BASE = "http://127.0.0.1:9/api/order";
	private static final String PAY_BASE = "http://127.0.0.1:9/api/payment";

	@Autowired
	private RestTemplate restTemplate;

	@Autowired
	private CoordinatorService coordinatorService;

	private MockRestServiceServer server;

	@BeforeEach
	void setUp() {
		server = MockRestServiceServer.bindTo(restTemplate).build();
	}

	@AfterEach
	void tearDown() {
		server.verify();
	}

	@Test
	void happyPath_bothPrepareThenCommit() {
		server.expect(requestTo(ORDER_BASE + "/prepare"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("PREPARED"));
		server.expect(requestTo(PAY_BASE + "/prepare"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("PREPARED"));
		server.expect(requestTo(ORDER_BASE + "/commit"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("COMMITTED"));
		server.expect(requestTo(PAY_BASE + "/commit"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("COMMITTED"));

		StartTransactionResponse r = coordinatorService.startTransaction(new StartTransactionRequest());

		assertThat(r.overallResult()).isEqualTo("COMMITTED");
		assertThat(r.orderStatus()).isEqualTo("COMMITTED");
		assertThat(r.paymentStatus()).isEqualTo("COMMITTED");
	}

	@Test
	void paymentPrepareFails_rollbacksBoth() {
		StartTransactionRequest req = new StartTransactionRequest();
		req.setPaymentPrepareFail(true);

		server.expect(requestTo(ORDER_BASE + "/prepare"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("PREPARED"));
		server.expect(requestTo(PAY_BASE + "/prepare?fail=true"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(withStatus(HttpStatus.INTERNAL_SERVER_ERROR));
		server.expect(requestTo(ORDER_BASE + "/rollback"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("ROLLED_BACK"));
		server.expect(requestTo(PAY_BASE + "/rollback"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("ROLLED_BACK"));

		StartTransactionResponse r = coordinatorService.startTransaction(req);

		assertThat(r.overallResult()).isEqualTo("ROLLED_BACK");
		assertThat(r.orderStatus()).isEqualTo("ROLLED_BACK");
		assertThat(r.paymentStatus()).isEqualTo("ROLLED_BACK");
	}

	@Test
	void orderCommitFails_skipsPaymentCommitAndRollbacksBoth() {
		StartTransactionRequest req = new StartTransactionRequest();
		req.setOrderCommitFail(true);

		server.expect(requestTo(ORDER_BASE + "/prepare"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("PREPARED"));
		server.expect(requestTo(PAY_BASE + "/prepare"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("PREPARED"));
		server.expect(requestTo(ORDER_BASE + "/commit?fail=true"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(withStatus(HttpStatus.INTERNAL_SERVER_ERROR));
		server.expect(requestTo(ORDER_BASE + "/rollback"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("ROLLED_BACK"));
		server.expect(requestTo(PAY_BASE + "/rollback"))
				.andExpect(method(HttpMethod.POST))
				.andRespond(jsonOk("ROLLED_BACK"));

		StartTransactionResponse r = coordinatorService.startTransaction(req);

		assertThat(r.overallResult()).isEqualTo("ROLLED_BACK");
		assertThat(r.orderStatus()).isEqualTo("ROLLED_BACK");
		assertThat(r.paymentStatus()).isEqualTo("ROLLED_BACK");
	}

	private static ResponseCreator jsonOk(String status) {
		String body = "{\"status\":\"" + status + "\",\"message\":\"ok\"}";
		return withSuccess(body, MediaType.APPLICATION_JSON);
	}
}
