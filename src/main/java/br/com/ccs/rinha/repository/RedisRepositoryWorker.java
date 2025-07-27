package br.com.ccs.rinha.repository;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;

@Component
public class RedisRepositoryWorker {

    private final Logger log = LoggerFactory.getLogger(RedisRepositoryWorker.class);
    private final RedisPaymentRepository repository;
    private final ArrayBlockingQueue<PaymentRequest> queue = new ArrayBlockingQueue<>(5000);
    private final int workers;

    public RedisRepositoryWorker(RedisPaymentRepository repository) {
        this.repository = repository;
        this.workers = Integer.parseInt(System.getenv("REDIS_WORKERS"));

        log.info("RedisRepositoryWorker initialized with {} workers", workers);
    }

    @PostConstruct
    public void init() {
        for (int i = 0; i < workers; i++) {
            startWorker(i);
        }
    }

    private void startWorker(int workerIndex) {
        Thread.ofVirtual().name("Redis-Worker-" + workerIndex)
                .start(() -> {
                    log.info("Starting Redis-Worker-{}", workerIndex);

                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            repository.store(queue.take());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    log.info("Redis-Worker-{} started", workerIndex);
                });
    }

    public void offer(PaymentRequest request) {
        queue.offer(request);
    }
}
