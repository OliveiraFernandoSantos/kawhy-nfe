package br.com.actionsys.kawhynfe.infra;

import br.com.actionsys.kawhycommons.Constants;
import br.com.actionsys.kawhycommons.infra.license.LicenseService;
import br.com.actionsys.kawhycommons.infra.util.DateUtil;
import br.com.actionsys.kawhycommons.integration.IntegrationContext;
import br.com.actionsys.kawhycommons.types.KawhyType;
import br.com.actionsys.kawhynfe.Orchestrator;
import br.com.actionsys.kawhynfe.Schedule;
import br.com.actionsys.kawhynfe.infra.control.ControlService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;

import java.io.File;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SpringBootTest
@Slf4j
@MockBean(Schedule.class)
public class ControlService_insertStaticTest {
    @Autowired
    private ControlService controlService;
    @Autowired
    private Orchestrator orchestrator;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @SpyBean
    private LicenseService licenseService;

    @Value("/xml/integration/procNFe_v4.00_completo.xml")
    private Resource xmlResource;

    @Value("xml/integration/ProcEvento_StaticCancel.xml")
    private Resource xmlCancelResource;

    public void before() throws Exception {
        Mockito.doReturn(true).when(licenseService).validateCnpj(Mockito.any());
        Mockito.doReturn(true).when(licenseService).validateCnpjs(Mockito.any());

        File resource = xmlResource.getFile();
        orchestrator.processItem(new IntegrationContext(resource));
    }

    @Test
    @Sql({"/data.sql"})
    public void validarTabelaKwnfe_controle() throws Exception {
        before();

        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72317 where nqq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nqq720652").toString().trim());//id
        Assertions.assertEquals("135231313568839", row.get("nqq720841").toString().trim());//protocolo
        Assertions.assertEquals(22, ((BigDecimal) row.get("nqq720708")).intValue());//nnf
        Assertions.assertEquals("01", row.get("nqq720707").toString().trim());//serie
        Assertions.assertEquals("56493877000116", row.get("nqq720804").toString().trim());//cnpj_emit
        Assertions.assertEquals("SP", row.get("nqq720814").toString().trim());//uf_emit
        Assertions.assertEquals("RIO CLARO", row.get("nqq720812").toString().trim());//xmun_emit
        Assertions.assertEquals("08062665000565", row.get("nqq720842").toString().trim());//cnpj_dest
        Assertions.assertNull(row.get("nqq720854"));//cpf_dest
        Assertions.assertEquals("CE", row.get("nqq720856").toString().trim());//uf_dest
        Assertions.assertEquals("Piracicaba", row.get("nqq720813").toString().trim());//xmun_dest
        Assertions.assertEquals("2023-08-10", row.get("nqq720709").toString().trim());//demi
        Assertions.assertEquals("100", row.get("nqq720961").toString().trim());//cstatussefaz
        Assertions.assertEquals("Autorizado o uso da NF-e", row.get("nqq720833").toString().trim());//xmotivo
        Assertions.assertEquals(DateUtil.formatDateToDb(new Date()), row.get("nqq720962").toString().trim());//Data da consulta Sefaz
//        Assertions.assertEquals("13:52:48", row.get("nqq720963").toString().trim());//Hora da consulta Sefaz
        Assertions.assertEquals("temp/ActionSys/KaWhys/Arquivos/Saida/NFe/procNFe_v4.00_completo.xml", row.get("nqq720973").toString().trim());//Caminho da pasta de saída
        Assertions.assertEquals(DateUtil.formatDateToDb(new Date()), row.get("nqq720972").toString().trim());//Data da Recepção do arquivo
//        Assertions.assertEquals(" ", row.get("nqq720966").toString().trim());//Data de entrada
        Assertions.assertEquals(Constants.KAWHY, row.get("nqq720863").toString().trim());//audit_usuario
        Assertions.assertEquals(KawhyType.KAWHY_NFE.getAuditName(), row.get("nqq720870").toString().trim());//audit_programa
        Assertions.assertEquals(DateUtil.formatDateToDb(new Date()), row.get("nqq720881").toString().trim());//audit_data
//        Assertions.assertEquals("13:52:48", row.get("nqq720874").toString().trim());//audit_hora
//        Assertions.assertEquals("68337658000127", row.get("nqq721103").toString().trim());//cnpj_transport
        Assertions.assertEquals("4.00", row.get("nqq721107").toString().trim());//datechgstatus07
        Assertions.assertFalse(row.get("nqq720980").toString().isEmpty());//cstatus01
        Assertions.assertFalse(row.get("nqq720983").toString().isEmpty());//cstatus02
        Assertions.assertFalse(row.get("nqq720986").toString().isEmpty());//cstatus03
        Assertions.assertFalse(row.get("nqq720989").toString().isEmpty());//cstatus04
        Assertions.assertFalse(row.get("nqq720992").toString().isEmpty());//cstatus05
        Assertions.assertFalse(row.get("nqq721101").toString().isEmpty());//cstatus06
        Assertions.assertFalse(row.get("nqq721105").toString().isEmpty());//cstatus07
        Assertions.assertFalse(row.get("nqq721108").toString().isEmpty());//cstatus08
        Assertions.assertFalse(row.get("nqq721108").toString().isEmpty());//cstatus09
        Assertions.assertFalse(row.get("nqq721114").toString().isEmpty());//cstatus10
        Assertions.assertFalse(row.get("nqq721117").toString().isEmpty());//cstatus11
        Assertions.assertFalse(row.get("nqq721120").toString().isEmpty());//cstatus12
        Assertions.assertFalse(row.get("nqq721123").toString().isEmpty());//cstatus13
        Assertions.assertFalse(row.get("nqq721126").toString().isEmpty());//cstatus14
        Assertions.assertFalse(row.get("nqq721129").toString().isEmpty());//cstatus15
        Assertions.assertFalse(row.get("nqq721132").toString().isEmpty());//cstatus16
        Assertions.assertFalse(row.get("nqq721135").toString().isEmpty());//cstatus17
        Assertions.assertFalse(row.get("nqq72138").toString().isEmpty());//cstatus18
        Assertions.assertFalse(row.get("nqq72141").toString().isEmpty());//cstatus19
        Assertions.assertFalse(row.get("nqq72144").toString().isEmpty());//cstatus20
//        Assertions.assertEquals(" ", row.get("nqq721138").toString().trim());//strguser01
//        Assertions.assertEquals(" ", row.get("nqq72155").toString().trim());//numbuser01
//        Assertions.assertEquals(" ", row.get("nqq72156").toString().trim());//numbuser02
//        Assertions.assertEquals(" ", row.get("nqq72160").toString().trim());//tiponfe  preencher no xml para testes nfeProc/NFe/infNFe/transp/retTransp/CFOP
//        Assertions.assertEquals("XML", row.get("nqq72162").toString().trim());//statusnfe
//        Assertions.assertEquals("273737", row.get("nqq72163").toString().trim());//autorinfe
//        Assertions.assertEquals("1", row.get("nqq72164").toString().trim());//qtysum
        Assertions.assertEquals(1243.5, ((BigDecimal) row.get("nqq72165")).doubleValue());//vlrnf
//        Assertions.assertEquals(" ", row.get("nqq72166").toString().trim());//umedida
//        Assertions.assertEquals(" ", row.get("nqq72167").toString().trim());//comercial
//        Assertions.assertEquals(" ", row.get("nqq72168").toString().trim());//filial
//        Assertions.assertEquals(" ", row.get("nqq72171").toString().trim());//codigoerp
//        Assertions.assertEquals(" ", row.get("nqq721173").toString().trim());//canc_prot
//        Assertions.assertEquals(" ", row.get("nqq721174").toString().trim());//canc_id
//        Assertions.assertEquals(" ", row.get("nqq721175").toString().trim());//canc_motivo
//        Assertions.assertEquals(" ", row.get("nqq721176").toString().trim());//canc_data
//        Assertions.assertEquals(" ", row.get("nqq721177").toString().trim());//canc_hora
    }

    @Test
    @Sql({"/data.sql"})
    public void cancelNFeTest() throws Exception {
        before();

        File resource = xmlCancelResource.getFile();
        IntegrationContext item = new IntegrationContext(resource);
        item.setId("NFe35230856493877000116550010002073651002155823");

        controlService.updateToCancel(item);

        String valueDB = jdbcTemplate.queryForObject("select nqq72162 from fq72317 where nqq720652 like 'NFe35230856493877000116550010002073651002155823%'", String.class);

        Assertions.assertEquals(Constants.DESCRIPTION_CANCEL_DOCUMENT, valueDB.trim());
    }

}
