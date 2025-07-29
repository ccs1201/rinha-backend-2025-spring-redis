package br.com.ccs.rinha.service;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.repository.ReactiveRedisPaymentRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class PaymentProcessorClientSharding {
    private static final Logger log = LoggerFactory.getLogger(PaymentProcessorClientSharding.class);

    private final ReactiveRedisPaymentRepository redisRepositoryWorker;
    private final String defaultUrl;
    private final String fallbackUrl;
    private final WebClient webClient;
    private final int retries;
    private final int timeOut;
    private final int workers;
    private final int queueCapacity;
    private final AtomicInteger index = new AtomicInteger(0);

    private final ArrayBlockingQueue<PaymentRequest>[] queues;

    public PaymentProcessorClientSharding(
            ReactiveRedisPaymentRepository redisRepositoryWorker,
            WebClient webClient,
            @Value("${payment-processor.default.url}") String defaultUrl,
            @Value("${payment-processor.fallback.url}") String fallbackUrl) {

        this.redisRepositoryWorker = redisRepositoryWorker;
        this.webClient = webClient;
        this.defaultUrl = defaultUrl.concat("/payments");
        this.fallbackUrl = fallbackUrl.concat("/payments");

        this.retries = Integer.parseInt(System.getenv("PAYMENT_PROCESSOR_MAX_RETRIES"));
        this.timeOut = Integer.parseInt(System.getenv("PAYMENT_PROCESSOR_REQUEST_TIMEOUT"));
        this.workers = Integer.parseInt(System.getenv("PAYMENT_PROCESSOR_WORKERS"));
        this.queueCapacity = Integer.parseInt(System.getenv("PAYMENT_QUEUE"));

        this.queues = new ArrayBlockingQueue[workers];

        log.info("Default service URL: {}", this.defaultUrl);
        log.info("Fallback service URL: {}", this.fallbackUrl);
        log.info("Request timeout: {}", timeOut);
        log.info("Max retries: {}", retries);
        log.info("Payment processor Workers: {}", workers);
        log.info("Queue capacity: {}", queueCapacity);
    }

    @PostConstruct
    public void init() {
        for (int i = 0; i < workers; i++) {
            queues[i] = new ArrayBlockingQueue<>(queueCapacity, false);
            startProcessQueue(i, queues[i]);
        }
    }

    private void startProcessQueue(int workerIndex, ArrayBlockingQueue<PaymentRequest> queue) {
        log.info("Starting payment-processor-worker-{}", workerIndex);
        Thread.ofVirtual().name("payment-processor-" + workerIndex).start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    processWithRetry(queue.take());
                } catch (InterruptedException e) {
                    log.error("worker: {} has error: {}", Thread.currentThread().getName(), e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            }
        });

        log.info("payment-processor-worker-{} started", workerIndex);
    }

    public void processPayment(PaymentRequest paymentRequest) {
        boolean accepted = queues[index.getAndIncrement()].offer(paymentRequest);

        if (!accepted) {
            log.error("Payment rejected by queue {}", index);
        }

        if (index.get() >= queues.length) {
            index.set(0);
        }
    }

    private void processWithRetry(PaymentRequest paymentRequest) {
        for (int i = 0; i < retries; i++) {
            if (Boolean.TRUE.equals(postToDefault(paymentRequest))) {
                save(paymentRequest);
                return;
            }

            if (Boolean.TRUE.equals(postToFallback(paymentRequest))) {
                save(paymentRequest);
                return;
            }
        }
        processPayment(paymentRequest);
    }

    private Boolean postToDefault(PaymentRequest paymentRequest) {
        paymentRequest.setDefaultTrue();

        return webClient.post()
                .uri(defaultUrl)
                .bodyValue(paymentRequest.getJson())
                .exchangeToMono(resp -> Mono.just(resp.statusCode().is2xxSuccessful()))
                .timeout(Duration.ofMillis(timeOut))
//                .doOnError(e -> log.error("default has error: {}", e.getMessage()))
                .onErrorReturn(Boolean.FALSE)
                .block();
    }

    private Boolean postToFallback(PaymentRequest paymentRequest) {
        paymentRequest.setDefaultFalse();

        return webClient.post()
                .uri(fallbackUrl)
                .bodyValue(paymentRequest.getJson())
                .exchangeToMono(resp -> Mono.just(resp.statusCode().is2xxSuccessful()))
                .timeout(Duration.ofMillis(timeOut))
//                .doOnError(e -> log.error("fallback has error: {}", e.getMessage()))
                .onErrorReturn(Boolean.FALSE)
                .block();
    }

    private void save(PaymentRequest request) {
        redisRepositoryWorker.store(request);
    }
}
