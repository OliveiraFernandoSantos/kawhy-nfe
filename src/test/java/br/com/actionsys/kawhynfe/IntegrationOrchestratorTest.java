package br.com.actionsys.kawhynfe;

import br.com.actionsys.kawhycommons.Constants;
import br.com.actionsys.kawhycommons.infra.exception.DuplicateFileException;
import br.com.actionsys.kawhycommons.infra.exception.IgnoreFileException;
import br.com.actionsys.kawhycommons.infra.exception.OtherFileException;
import br.com.actionsys.kawhycommons.infra.license.LicenseService;
import br.com.actionsys.kawhycommons.infra.util.FilesUtil;
import br.com.actionsys.kawhycommons.integration.IntegrationContext;
import br.com.actionsys.kawhycommons.types.KawhyType;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@SpringBootTest
@Slf4j
@MockBean(Schedule.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IntegrationOrchestratorTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @SpyBean
    private Orchestrator orchestrator;
    @SpyBean
    private LicenseService licenseService;

    @Value("xml/integration/35230312096667000119550010000253911351258359-nfe.xml")
    private Resource resourceDocument;
    @Value("xml/integration/ProcEvento_35230312096667000119550010000253911351258359.xml")
    private Resource resourceCancel;
    @Value("xml/integration/35230312096667000119550010000253911351258359-nfe_devolucao.xml")
    private Resource resourceDevolucao;
    @Value("xml/integration/35230312096667000119550010000253911351258359-nfe_erro.txt")
    private Resource resourceDocErro;
    @Value("xml/integration/35230360659166000146550010000911131928840224-nfe_charset.xml")
    private Resource resourceCharset;
    @Value("xml/integration/35230360659166000146550010000911131928840223-nfe_caracter_especial.xml")
    private Resource resourceCaracterEspecial;

    @BeforeEach
    public void before() {
        Mockito.doReturn(true).when(licenseService).validateCnpj(Mockito.any());
        Mockito.doReturn(true).when(licenseService).validateCnpjs(Mockito.any());
    }

    @Test
    public void isCancelTest() throws Exception {
        Assertions.assertEquals(false, orchestrator.isCancel(new IntegrationContext(resourceDocument.getFile())));
        Assertions.assertEquals(true, orchestrator.isCancel(new IntegrationContext(resourceCancel.getFile())));
        Assertions.assertEquals(false, orchestrator.isCancel(new IntegrationContext(resourceDevolucao.getFile())));
    }

    @Test
    public void isDocumentTest() throws Exception {
        Assertions.assertEquals(true, orchestrator.isDocument(new IntegrationContext(resourceDocument.getFile())));
        Assertions.assertEquals(false, orchestrator.isDocument(new IntegrationContext(resourceCancel.getFile())));
        Assertions.assertEquals(true, orchestrator.isDocument(new IntegrationContext(resourceDevolucao.getFile())));
    }

    @Test
    public void isDevolucaoTest() throws Exception {
        Assertions.assertEquals(false, orchestrator.isDevolucao(new IntegrationContext(resourceDocument.getFile())));
        Assertions.assertEquals(false, orchestrator.isDevolucao(new IntegrationContext(resourceCancel.getFile())));
        Assertions.assertEquals(true, orchestrator.isDevolucao(new IntegrationContext(resourceDevolucao.getFile())));
    }

    @Test
    @Sql({"/data.sql"})
    public void processDocumentApprovedTest() throws Exception {
        IntegrationContext itemDoc = new IntegrationContext(resourceDocument.getFile());

        Assertions.assertDoesNotThrow(() -> orchestrator.processDocumentFile(itemDoc));

        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select nqq720961 from fq72317 where nqq720652 like '%35230312096667000119550010000253911351258359%'");//nfe_id
        Map<String, Object> row = result.get(0);

        //Nota aprovada, o status deve ser '100'
        Assertions.assertEquals("100", row.get("nqq720961").toString().trim());
    }

    @Test
    @Sql({"/data.sql"})
    public void processDocumentCharsetTest() throws Exception {
        IntegrationContext itemDoc = new IntegrationContext(resourceCharset.getFile());
        Assertions.assertDoesNotThrow(() -> orchestrator.processDocumentFile(itemDoc));

        List<Map<String, Object>> result = jdbcTemplate.queryForList ("select nqq720961 from fq72317 where nqq720652 like '%35230360659166000146550010000911131928840224%'");//nfe_id

         Map<String, Object> row = result.get(0);

        //Nota aprovada, o status deve ser '100'
        Assertions.assertEquals("100", row.get("nqq720961").toString().trim());
    }

    @Test
    @Sql({"/data.sql"})
    public void processDocumentWithSpecialCharactersTest() throws Exception {
       IntegrationContext itemDoc = new IntegrationContext(resourceCaracterEspecial.getFile());
       Assertions.assertDoesNotThrow(() -> orchestrator.processDocumentFile(itemDoc));

       List<Map<String, Object>> result = jdbcTemplate.queryForList("select nqq720961 from fq72317 where nqq720652 like '%35230360659166000146550010000911131928840223%'");//nfe_id

       Map<String, Object> row = result.get(0);

       //Nota aprovada, o status deve ser '100'
       Assertions.assertEquals("100", row.get("nqq720961").toString().trim());
    }
 

    @Test
    @Sql({"/data.sql"})
    public void processDuplicatedDocumentTest() throws Exception {
        IntegrationContext itemDoc = new IntegrationContext(resourceDocument.getFile());

        //Nota processada pela primeira
        Assertions.assertDoesNotThrow(() -> orchestrator.processDocumentFile(itemDoc));

        //Nota reprocessada
        Assertions.assertThrows(DuplicateFileException.class, () -> orchestrator.processDocumentFile(itemDoc));
    }

    @Test
    @Sql({"/data.sql"})
    public void processFileCancelBeforeNfeTest() throws Exception {
        //Evento de cancelamento antes de receber a NF
        IntegrationContext itemCancel = new IntegrationContext(resourceCancel.getFile());

        Assertions.assertThrows(IgnoreFileException.class, () -> orchestrator.processCancelFile(itemCancel));
    }

    @Test
    @Sql({"/data.sql"})
    public void processFileCancelAfterDocNfeTest() throws Exception {
        //Nota fiscal
        IntegrationContext itemDoc = new IntegrationContext(resourceDocument.getFile());
        Assertions.assertDoesNotThrow(() -> orchestrator.processDocumentFile(itemDoc));

        //Evento de cancelamento
        IntegrationContext itemCancel = new IntegrationContext(resourceCancel.getFile());

        Assertions.assertDoesNotThrow(() -> orchestrator.processCancelFile(itemCancel));

        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select nqq720961 from fq72317 where nqq720652 like '%35230312096667000119550010000253911351258359%'");//nfe_id
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals(Constants.CSTATUS_SEFAZ_CACELAMENTO, row.get("nqq720961").toString().trim());
    }

    @Test
    @Sql({"/data.sql"})
    public void processDevolucaoFlagFalseTest() throws Exception {
        Mockito.doReturn(false).when(licenseService).hasDevolucao(Mockito.any());
        IntegrationContext itemDevolucao = new IntegrationContext(resourceDevolucao.getFile());

        Assertions.assertThrows(OtherFileException.class, () -> orchestrator.processDocumentFile(itemDevolucao));
    }

    @Test
    @Sql({"/data.sql"})
    public void processDevelocaoFlagTrueTest() throws Exception {
        Mockito.doReturn(true).when(licenseService).hasDevolucao(Mockito.any());

        IntegrationContext itemDevolucao = new IntegrationContext(resourceDevolucao.getFile());

        Assertions.assertDoesNotThrow(() -> orchestrator.processDocumentFile(itemDevolucao));

        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72317 where nqq720652 like '%NFe35230312096667000119550010000253911351258359%'");//nfe_id
        Map<String, Object> row = result.get(0);

        //Nota aprovada, o status deve ser '100'
        Assertions.assertEquals("100", row.get("nqq720961").toString().trim());
    }

    @Test
    public void processItemTest() throws Exception {
        Assertions.assertThrows(OtherFileException.class,
                () -> orchestrator.processItem(new IntegrationContext(resourceDocErro.getFile())));
    }

    @Test
    @Sql({"/data.sql"})
    public void processRollback() throws Exception {
        Mockito.doReturn(true).when(licenseService).hasDevolucao(Mockito.any());

        Mockito.doReturn(null).when(orchestrator).getKawhyType();

        IntegrationContext itemDevolucao = new IntegrationContext(resourceDocument.getFile());
        Assertions.assertThrows(NullPointerException.class, () -> orchestrator.processItem(itemDevolucao));

        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72317 where nqq720652 like '%NFe35230312096667000119550010000253911351258359%'");
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void processDocumentWiyhEncodeISO_8859_1 () throws Exception {
        IntegrationContext integrationContext  = new IntegrationContext();
        integrationContext.setFile(resourceCaracterEspecial.getFile());

       Assertions.assertDoesNotThrow(() -> orchestrator.processItem(integrationContext));
    }
}
