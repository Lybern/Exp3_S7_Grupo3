package cl.duoc.bancoxyz.auditoria.controller;

import cl.duoc.bancoxyz.auditoria.messaging.TransaccionAuditoriaListener;
import cl.duoc.bancoxyz.auditoria.messaging.TransaccionEvent;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/auditoria")
@Tag(name = "Auditoría de Eventos JMS", description = "Consulta de eventos bancarios asíncronos procesados")
public class AuditoriaController {

    private final TransaccionAuditoriaListener listener;

    public AuditoriaController(TransaccionAuditoriaListener listener) {
        this.listener = listener;
    }

    @Operation(summary = "Listar eventos consumidos desde la cola ActiveMQ")
    @GetMapping("/eventos")
    public ResponseEntity<Map<String, Object>> obtenerEventosAuditoria() {
        List<TransaccionEvent> eventos = listener.getEventosRecibidos();
        return ResponseEntity.ok(Map.of(
                "totalEventos", eventos.size(),
                "colaJms", "transacciones.bancarias",
                "broker", "Apache ActiveMQ Classic (tcp://localhost:61616)",
                "eventos", eventos
        ));
    }
}
