package br.com.actionsys.kawhynfe;

import br.com.actionsys.kawhycommons.erro.DbErrosService;
import br.com.actionsys.kawhycommons.infra.dbsavexml.DbSaveXmlService;
import br.com.actionsys.kawhycommons.infra.license.DecryptService;
import br.com.actionsys.kawhycommons.infra.license.LicenseService;
import br.com.actionsys.kawhyimport.metadata.ImportService;
import br.com.actionsys.kawhynfe.infra.control.ControlService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;

import java.util.ArrayList;

@SpringBootTest(classes = {LicenseService.class, DecryptService.class })
@Slf4j
@MockBean({Schedule.class,DbErrosService.class, ImportService.class, DbSaveXmlService.class, ControlService.class})

public class Process_validateLicenseModuloTest {

    @MockBean
    private LicenseService licenseService;
    @SpyBean
    private Orchestrator orchestrator;

    @Test
    public void trueTest() {
        Mockito.doReturn(true).when(licenseService).validateLicenseModule(Mockito.any());
        Mockito.doReturn(new ArrayList<>()).when(orchestrator).getFiles();

        Assertions.assertDoesNotThrow(() -> orchestrator.process());

        Mockito.verify(orchestrator, Mockito.times(1)).getFiles();
    }

    @Test
    public void falseTest() {
        Mockito.doReturn(false).when(licenseService).validateLicenseModule(Mockito.any());
        Mockito.doReturn(new ArrayList<>()).when(orchestrator).getFiles();


        Assertions.assertDoesNotThrow(() -> orchestrator.process());

        Mockito.verify(orchestrator, Mockito.never()).getFiles();
    }

    @Test
    public void exceptionTest() {
        Mockito.doThrow(new RuntimeException("Licença não encontrada")).when(licenseService).validateLicenseModule(Mockito.any());

        Assertions.assertThrows(RuntimeException.class, () -> orchestrator.process());

        Mockito.verify(orchestrator, Mockito.never()).getFiles();
    }
}
