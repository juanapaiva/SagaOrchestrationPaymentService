package br.com.microservices.orchestrated.paymentservice.core.utils;

import br.com.microservices.orchestrated.paymentservice.core.dto.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class JsonUtil {

    private final ObjectMapper objectMapper;

    public String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return "";
        }
    }

    public Event toEvent(String json) {
        try {
            return objectMapper.readValue(json, Event.class);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }
}
