package br.com.ccs.rinha.repository;

import br.com.ccs.rinha.api.model.input.PaymentRequest;
import br.com.ccs.rinha.api.model.output.PaymentSummary;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static java.util.Objects.isNull;

@Component
public class ReactiveRedisPaymentRepository {

    private static final String PAYMENTS = "payments";

    private final ReactiveRedisTemplate<String, String> redisTemplate;


    public ReactiveRedisPaymentRepository(ReactiveRedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void store(PaymentRequest paymentRequest) {
        String data = DataBuilder.build(paymentRequest);

        double score = paymentRequest.requestedAt.toEpochMilli();

        redisTemplate.opsForZSet()
                .add(PAYMENTS, data, score)
                .thenReturn(paymentRequest)
                .subscribe();
    }

    public PaymentSummary getSummary(Instant from, Instant to) {

        if (isNull(from)) {
            from = Instant.now().minus(5, ChronoUnit.MINUTES);
        }

        if (isNull(to)) {
            to = Instant.now();
        }

        var range = Range.of(Range.Bound.inclusive((double) from.toEpochMilli()),
                Range.Bound.inclusive((double) to.toEpochMilli()));

        return redisTemplate.opsForZSet()
                .rangeByScore(PAYMENTS, range)
                .collectList()
                .map(this::calculateSummary)
                .block();
    }

    private PaymentSummary calculateSummary(List<String> payments) {
        if (payments == null || payments.isEmpty()) {
            return new PaymentSummary(new PaymentSummary.Summary(0, BigDecimal.ZERO),
                    new PaymentSummary.Summary(0, BigDecimal.ZERO));
        }

        long defaultCount = 0;
        long fallbackCount = 0;
        long defaultAmount = 0L;
        long fallbackAmount = 0L;

        for (String payment : payments) {
            String[] parts = payment.split(":");
            long amount = Long.parseLong(parts[1]);
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
        redisTemplate.delete(PAYMENTS).block();
    }

    private static final class DataBuilder {

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
