package br.com.ccs.rinha.api.controller;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.api.model.output.PaymentSummary;
import br.com.ccs.rinha.repository.RedisPaymentRepository;
import br.com.ccs.rinha.service.PaymentProcessorClientServiceBlocking;
import jakarta.annotation.PreDestroy;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

@RestController
public class PaymentController {

    private final PaymentProcessorClientServiceBlocking client;
    private final RedisPaymentRepository repository;
    private final ExecutorService executor;

    public PaymentController(PaymentProcessorClientServiceBlocking client,
                             RedisPaymentRepository repository,
                             ThreadPoolExecutor executor) {
        this.client = client;
        this.repository = repository;
        this.executor = executor;
    }

    @PostMapping("/payments")
    public void createPayment(@RequestBody String request) {
        executor.submit(() -> client.processPayment(PaymentRequest.parse(request)), executor);

    }

    @GetMapping("/payments-summary")
    public PaymentSummary getPaymentsSummary(@RequestParam(required = false) Instant from,
                                             @RequestParam(required = false) Instant to) {

        return repository.getSummary(from, to);
    }

    @PostMapping("/purge-payments")
    public ResponseEntity<Void> purgePayments() {
        repository.purge();
        return ResponseEntity.ok().build();
    }

    @PreDestroy
    public void shutdown() {
        if (executor.isShutdown()) return;
        executor.shutdownNow();
    }

}
