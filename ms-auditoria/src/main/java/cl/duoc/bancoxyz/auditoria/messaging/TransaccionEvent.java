package cl.duoc.bancoxyz.auditoria.messaging;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TransaccionEvent implements Serializable {
    private String transaccionId;
    private Long cuentaOrigenId;
    private Long cuentaDestinoId;
    private Long monto;
    private String tipoOperacion; // RETIRO, TRANSFERENCIA
    private String canal;         // MOVIL, WEB, ATM
    private String estado;        // EXITOSA, FALLIDA, CONTINGENCIA
    private String fechaHora;
    private String detalle;
}
