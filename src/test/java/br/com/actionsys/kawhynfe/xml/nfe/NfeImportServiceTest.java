package br.com.actionsys.kawhynfe.xml.nfe;

import br.com.actionsys.kawhycommons.infra.license.LicenseService;
import br.com.actionsys.kawhycommons.infra.util.APathUtil;
import br.com.actionsys.kawhycommons.integration.IntegrationContext;
import br.com.actionsys.kawhycommons.integration.metadata.MetadataService;
import br.com.actionsys.kawhyimport.metadata.ImportService;
import br.com.actionsys.kawhynfe.Schedule;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

@Slf4j
@SpringBootTest
@MockBean(Schedule.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NfeImportServiceTest {

    @SpyBean
    private LicenseService licenseService;
    @Value("classpath:/metadados/teste")
    private Resource metadadosParaTeste;
    @Value("classpath:xml/nfes")
    private Resource xmlResource;
    @Autowired
    private MetadataService metadataService;
    @Autowired
    private ImportService importService;

    @BeforeAll
    void before() throws Exception {
        Mockito.doReturn(true).when(licenseService).validateCnpj(Mockito.any());
        Mockito.doReturn(true).when(licenseService).validateCnpjs(Mockito.any());
        Mockito.doReturn(true).when(licenseService).validateLicenseModule(Mockito.any());

        try (Stream<Path> files = Files.walk(xmlResource.getFile().toPath(), 99).filter(Files::isRegularFile)) {
            files.forEach(this::importFile);
        } catch (IOException e) {
            throw new RuntimeException("Erro ao listar arquivos", e);
        }
    }

    @Test
    void validarProcessamentoDeMetadadosNfe() {

        Assertions.assertDoesNotThrow(() -> metadataService.validateFiles(
                metadadosParaTeste.getFile().toPath(),
                xmlResource.getFile().toPath())
        );
    }

    private void importFile(Path file) {
        try {
            log.info("Importado documento {}", file.getFileName());

            IntegrationContext item = new IntegrationContext(file.toFile());
            item.setId(APathUtil.execute(item.getDocument(), "nfeProc/NFe/infNFe/@Id"));

            importService.process(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
