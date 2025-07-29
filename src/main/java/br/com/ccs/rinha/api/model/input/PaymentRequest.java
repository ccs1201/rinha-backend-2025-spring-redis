package br.com.ccs.rinha.api.model.input;

import java.math.BigDecimal;
import java.time.Instant;

public final class PaymentRequest {
    public String correlationId;
    public BigDecimal amount;
    public Instant requestedAt;
    public boolean isDefault;

    private String json;

    public void setDefaultFalse() {
        this.isDefault = false;
    }

    public void setDefaultTrue() {
        this.isDefault = true;
    }

    public String getJson() {
        if (json == null) {
            toJson();
        }
        return json;
    }

    public static PaymentRequest parse(String json) {
        PaymentRequest req = new PaymentRequest();
        char[] chars = json.toCharArray();

        // correlationId
        int idx = json.indexOf("\"correlationId\":\"") + 17;
        int end = json.indexOf('"', idx);
        req.correlationId = json.substring(idx, end);

        // amount
        idx = json.indexOf("\"amount\":", end) + 9;
        while (chars[idx] == ' ' || chars[idx] == '\t') idx++; // pular espaços

        int startAmount = idx;
        while ((chars[idx] >= '0' && chars[idx] <= '9') || chars[idx] == '.' || chars[idx] == '-') idx++;

        req.amount = new BigDecimal(json.substring(startAmount, idx));

        // instante de chegada da requisição
        req.requestedAt = Instant.now();
        req.setDefaultFalse();

        return req;
    }

//    public static PaymentRequest parse(String json) {
//        PaymentRequest req = new PaymentRequest();
//        int startId = json.indexOf(":\"") + 2;
//        int endId = json.indexOf('"', startId);
//        req.correlationId = json.substring(startId, endId);
//        int startAmount = json.indexOf(":", endId) + 1;
//        int endAmount = json.indexOf('}', startAmount);
//        req.amount = new BigDecimal(json.substring(startAmount, endAmount).trim());
//        req.requestedAt = Instant.now();
//        req.setDefaultFalse();
//
//        return req;
//    }

    private void toJson() {
        var sb = new StringBuilder(128);
        json = sb.append("{")
                .append("\"correlationId\":\"").append(correlationId).append("\",")
                .append("\"amount\":").append(amount).append(",")
                .append("\"requestedAt\":\"").append(requestedAt).append("\"")
                .append("}")
                .toString();
    }
}