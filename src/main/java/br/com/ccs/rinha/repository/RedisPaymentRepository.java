package br.com.ccs.rinha.repository;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.api.model.output.PaymentSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import static java.util.Objects.isNull;

@Repository
public class RedisPaymentRepository {

    private static final Logger log = LoggerFactory.getLogger(RedisPaymentRepository.class);

    private final RedisTemplate<String, String> redisTemplate;
    private static final String PAYMENTS = "payments";

    public RedisPaymentRepository(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void store(PaymentRequest paymentRequest) {
        var data = DataBuilder.build(paymentRequest);

        redisTemplate
                .opsForZSet()
                .add(PAYMENTS, data, paymentRequest.requestedAt.toEpochMilli());

    }

    public void storeBatch(List<PaymentRequest> requests) {
        var start = System.nanoTime();

        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (PaymentRequest request : requests) {
                String data = DataBuilder.build(request);

                byte[] key = redisTemplate.getStringSerializer().serialize(PAYMENTS);
                byte[] value = redisTemplate.getStringSerializer().serialize(data);

                connection.zAdd(key, request.requestedAt.toEpochMilli(), value);
            }

            log.info("BATCH {} executed in {}ms", requests.size(),
                    String.format("%.3f", (System.nanoTime() - start) / 1_000_000F));

            return null;
        });
    }


    public PaymentSummary getSummary(Instant from, Instant to) {

        if (isNull(from)) {
            from = Instant.now().minus(5, ChronoUnit.MINUTES);
        }

        if (isNull(to)) {
            to = Instant.now();
        }
        var payments = redisTemplate.opsForZSet().rangeByScore(PAYMENTS, from.toEpochMilli(), to.toEpochMilli());

        return calculateSummary(payments);
    }

    private PaymentSummary calculateSummary(Set<String> payments) {
        if (payments == null || payments.isEmpty()) {
            return new PaymentSummary(new PaymentSummary.Summary(0, BigDecimal.ZERO), new PaymentSummary.Summary(0, BigDecimal.ZERO));
        }

        long defaultCount = 0;
        long fallbackCount = 0;
        long defaultAmount = 0L;
        long fallbackAmount = 0L;

        for (String payment : payments) {
            String[] parts = payment.split(":");
            long amount = Long.parseLong(parts[1]); // armazenar centavos
            boolean isDefault = Boolean.parseBoolean(parts[2]);

            if (isDefault) {
                defaultCount++;
                defaultAmount += amount;
            } else {
                fallbackCount++;
                fallbackAmount += amount;
            }
        }

        return new PaymentSummary(
                new PaymentSummary.Summary(defaultCount, new BigDecimal(defaultAmount).movePointLeft(2)),
                new PaymentSummary.Summary(fallbackCount, new BigDecimal(fallbackAmount).movePointLeft(2)));
    }

    public void purge() {
        redisTemplate.delete(PAYMENTS);
    }

    private final class DataBuilder {

        private static final ThreadLocal<StringBuilder> builderHolder = ThreadLocal.withInitial(() -> new StringBuilder(64));

        private DataBuilder() {
        }

        public static String build(PaymentRequest request) {
            StringBuilder sb = builderHolder.get();
            sb.setLength(0);

            sb.append(request.correlationId)
                    .append(':')
                    .append(request.amount.multiply(BigDecimal.valueOf(100)).longValueExact())
                    .append(':')
                    .append(request.isDefault);

            return sb.toString();
        }
    }

}