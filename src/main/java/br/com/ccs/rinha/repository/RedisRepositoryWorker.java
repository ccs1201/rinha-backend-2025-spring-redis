package br.com.ccs.rinha.repository;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class RedisRepositoryWorker {

    private final Logger log = LoggerFactory.getLogger(RedisRepositoryWorker.class);
    private final RedisPaymentRepository repository;
    private final LinkedBlockingQueue<PaymentRequest> queue = new LinkedBlockingQueue<>(5000);
    private final int workers;

    public RedisRepositoryWorker(RedisPaymentRepository repository) {
        this.repository = repository;
        this.workers = Integer.parseInt(System.getenv("REDIS_WORKERS"));

        for (int i = 0; i < workers; i++) {
            startWorker(i);
        }
    }

    private void startWorker(int workerIndex) {
        Thread.ofVirtual().name("Redis-Worker-" + workerIndex)
                .start(() -> {
                    int completed = 0;
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            if (queue.size() > 10) {
                                var payments = new ArrayList<PaymentRequest>(50);
                                queue.drainTo(payments, 50);
                                repository.storeBatch(payments);
                            }

                            var p = queue.take();
                            var start = System.nanoTime();
                            repository.store(p);
                            completed++;
                            long elapsedNanos = System.nanoTime() - start;
                            double elapsedMillis = elapsedNanos / 1_000_000.0;
                            log.info("Redis-Worker-{} tasks {} completed in {}ms remaining {}", workerIndex, completed, String.format("%.3f", elapsedMillis), queue.size());

                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
    }

    public void offer(PaymentRequest request) {
        queue.offer(request);
    }
}
