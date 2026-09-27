package cl.duoc.bancoxyz.auditoria.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class TransaccionAuditoriaListener {

    private static final Logger log = LoggerFactory.getLogger(TransaccionAuditoriaListener.class);
    private final List<TransaccionEvent> eventosRecibidos = new CopyOnWriteArrayList<>();

    @JmsListener(destination = "transacciones.bancarias")
    public void recibirEventoTransaccion(TransaccionEvent evento) {
        log.info("===============================================================");
        log.info("[MS-AUDITORIA] EVENTO RECIBIDO DESDE ACTIVEMQ");
        log.info("ID Transacción: {}", evento.getTransaccionId());
        log.info("Tipo Operación: {}", evento.getTipoOperacion());
        log.info("Canal:          {}", evento.getCanal());
        log.info("Monto:          ${}", evento.getMonto());
        log.info("Cuenta Origen:  {}", evento.getCuentaOrigenId());
        log.info("Cuenta Destino: {}", evento.getCuentaDestinoId());
        log.info("Estado:         {}", evento.getEstado());
        log.info("Fecha/Hora:     {}", evento.getFechaHora());
        log.info("Detalle:        {}", evento.getDetalle());
        log.info("===============================================================");

        eventosRecibidos.add(evento);
    }

    public List<TransaccionEvent> getEventosRecibidos() {
        return Collections.unmodifiableList(eventosRecibidos);
    }
}
