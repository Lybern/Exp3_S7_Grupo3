package cl.duoc.bancoxyz.core.messaging;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class TransaccionMessagingProducer {

    private static final Logger log = LoggerFactory.getLogger(TransaccionMessagingProducer.class);
    private static final String DESTINATION_QUEUE = "transacciones.bancarias";

    @Autowired
    private JmsTemplate jmsTemplate;

    // Bitacora de contingencia local en memoria ante caida del broker ActiveMQ o Circuito Abierto
    private final List<TransaccionEvent> transaccionesContingencia = new CopyOnWriteArrayList<>();

    @CircuitBreaker(name = "envioMensajeria", fallbackMethod = "fallbackEnvioMensaje")
    @Retry(name = "envioMensajeria", fallbackMethod = "fallbackEnvioMensaje")
    public void publicarTransaccion(TransaccionEvent evento) {
        try {
            log.info("[PRODUCER-JMS] Publicando evento transaccional '{}' en la cola '{}'", 
                    evento.getTransaccionId(), DESTINATION_QUEUE);
            jmsTemplate.convertAndSend(DESTINATION_QUEUE, evento);
            log.info("[PRODUCER-JMS] Transaccion '{}' publicada con exito en broker ActiveMQ", evento.getTransaccionId());
        } catch (Exception e) {
            fallbackEnvioMensaje(evento, e);
        }
    }

    // Metodo de Fallback cuando el broker ActiveMQ esta caido o el circuito esta ABIERTO
    public void fallbackEnvioMensaje(TransaccionEvent evento, Throwable ex) {
        log.error("[FALLBACK RESILIENCE4J] ActiveMQ no disponible o circuito ABIERTO. Causa: {}", ex.getMessage());
        evento.setEstado("CONTINGENCIA_PENDIENTE_BROKER");
        evento.setDetalle("Broker ActiveMQ inaccesible - Contingencia local: " + ex.getMessage());
        transaccionesContingencia.add(evento);
        log.warn("[FALLBACK RESILIENCE4J] Guardando evento '{}' en contingencia local. Total pendientes: {}", 
                evento.getTransaccionId(), transaccionesContingencia.size());
    }

    public List<TransaccionEvent> getTransaccionesContingencia() {
        return Collections.unmodifiableList(transaccionesContingencia);
    }
}
