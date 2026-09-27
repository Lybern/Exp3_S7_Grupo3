package cl.duoc.bancoxyz.auditoria;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.EnableJms;

@SpringBootApplication
@EnableJms
public class MsAuditoriaApplication {
    public static void main(String[] args) {
        SpringApplication.run(MsAuditoriaApplication.class, args);
    }
}
