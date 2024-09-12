package br.com.actionsys.kawhynfe.xml.nfe;

import br.com.actionsys.kawhycommons.infra.license.LicenseService;
import br.com.actionsys.kawhycommons.integration.IntegrationContext;
import br.com.actionsys.kawhynfe.Orchestrator;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;

import java.io.File;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@SpringBootTest
@MockBean(Schedule.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NfeServiceStaticTest {

    @Autowired
    private Orchestrator orchestrator;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Value("/xml/integration/procNFe_v4.00_completo.xml")
    private Resource xmlResource;
    @SpyBean
    private LicenseService licenseService;

    @BeforeAll
    @Sql({"/data.sql"})
    void before() throws Exception {
        Mockito.doReturn(true).when(licenseService).validateCnpj(Mockito.any());
        Mockito.doReturn(true).when(licenseService).validateCnpjs(Mockito.any());
        File resource = xmlResource.getFile();

        orchestrator.processDocumentFile(new IntegrationContext(resource));
    }

    @Test
    void validarTabelaKwnfe() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72300 where nfq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nfq720652").toString().trim());//id
        Assertions.assertEquals("22", row.get("nfq720702").toString().trim());//ideCuf
        Assertions.assertEquals(1, ((BigDecimal) row.get("nfq720703")).intValue());//ideCnf
        Assertions.assertEquals("1", row.get("nfq720704").toString().trim());//ideNatOp
        Assertions.assertNull(row.get("nfq720705"));//ideNatOp - Coluna não utilizada na versão 4.00
        Assertions.assertEquals("55", row.get("nfq720706").toString().trim());//ideMod
        Assertions.assertEquals("01", row.get("nfq720707").toString().trim());//ideSerie
        Assertions.assertEquals(22, ((BigDecimal) row.get("nfq720708")).intValue());//ideNnf
        Assertions.assertEquals("2023-08-10", row.get("nfq720709").toString().trim());//ideDhEmi
        Assertions.assertEquals("2023-08-10", row.get("nfq720710").toString().trim());//ideSaiEnt
        Assertions.assertEquals("14:02:09", row.get("nfq720711").toString().trim());//ideHsaiEnt
        Assertions.assertEquals(1, ((BigDecimal)  row.get("nfq720712")).intValue());//ideTpNf
        Assertions.assertEquals(3533403, ((BigDecimal)  row.get("nfq720713")).intValue());//ideCmunFg
        Assertions.assertEquals(5, ((BigDecimal)  row.get("nfq720714")).intValue());//ideTpImp
        Assertions.assertEquals("1", row.get("nfq720715").toString().trim());//ideTpEmis
        Assertions.assertEquals(3, ((BigDecimal) row.get("nfq720716")).intValue());//ideCdv
        Assertions.assertEquals(2, ((BigDecimal) row.get("nfq720717")).intValue());//ideTpAmb
        Assertions.assertEquals(3, ((BigDecimal) row.get("nfq720718")).intValue());//ideFinNfe
        Assertions.assertEquals(1, ((BigDecimal) row.get("nfq720719")).intValue());//ideProcEmi
        Assertions.assertEquals("4.00", row.get("nfq720720").toString().trim());//ideVerProc
        Assertions.assertEquals("2023-08-10T14:02:09", row.get("nfq720721").toString().trim());//ideDhCont
        Assertions.assertEquals("0", row.get("nfq720723").toString().trim());//ideXjust
        Assertions.assertEquals(1, ((BigDecimal) row.get("nfq720724")).intValue());//icmsVbc
        Assertions.assertEquals(2, ((BigDecimal) row.get("nfq720725")).intValue());//icmsVICMS
        Assertions.assertEquals(8, ((BigDecimal) row.get("nfq720726")).intValue());//icmsVbcst
        Assertions.assertEquals(9, ((BigDecimal) row.get("nfq720727")).intValue());//icmsVst
        Assertions.assertEquals(2, ((BigDecimal) row.get("nfq720728")).intValue());//icmsVprod
        Assertions.assertEquals(3, ((BigDecimal) row.get("nfq720729")).intValue());//icmsVfrete
        Assertions.assertEquals(4, ((BigDecimal) row.get("nfq720730")).intValue());//icmsVseg
        Assertions.assertEquals(5, ((BigDecimal) row.get("nfq720731")).intValue());//icmsVdesc
        Assertions.assertEquals(6, ((BigDecimal) row.get("nfq720732")).intValue());//icmsVii
        Assertions.assertEquals(7, ((BigDecimal) row.get("nfq720733")).intValue());//icmsVipi
        Assertions.assertEquals(9, ((BigDecimal) row.get("nfq720734")).intValue());//icmsProdVpis
        Assertions.assertEquals(1, ((BigDecimal) row.get("nfq720735")).intValue());//icmsVcofins
        Assertions.assertEquals(2, ((BigDecimal) row.get("nfq720736")).intValue());//icmsVoutro
        Assertions.assertEquals(1243, ((BigDecimal) row.get("nfq720737")).intValue());//icmsVnf
        Assertions.assertEquals(5, ((BigDecimal) row.get("nfq720738")).intValue());//icmsVserv
        Assertions.assertEquals(6, ((BigDecimal) row.get("nfq720739")).intValue());//icmsVbc
        Assertions.assertEquals(7, ((BigDecimal) row.get("nfq720740")).intValue());//icmsViss
        Assertions.assertEquals(8, ((BigDecimal) row.get("nfq720741")).intValue());//icmsVpis
        Assertions.assertEquals(9, ((BigDecimal) row.get("nfq720742")).intValue());//icmsServVcofins
        Assertions.assertEquals(1, ((BigDecimal) row.get("nfq720743")).intValue());//cRegTribVRetpis
        Assertions.assertEquals(2, ((BigDecimal) row.get("nfq720744")).intValue());//cRegTribVretcofins
        Assertions.assertEquals(3, ((BigDecimal) row.get("nfq720745")).intValue());//cRegTribVretcsll
        Assertions.assertEquals(4, ((BigDecimal) row.get("nfq720746")).intValue());//cRegTribVretcsll
        Assertions.assertEquals(5, ((BigDecimal) row.get("nfq720747")).intValue());//cRegTribVirrf
        Assertions.assertEquals(6, ((BigDecimal) row.get("nfq720748")).intValue());//cRegTribVbcRetPrev
        Assertions.assertEquals(7, ((BigDecimal) row.get("nfq720749")).intValue());//cRegTribVRetPrev
        Assertions.assertEquals("9", row.get("nfq720750").toString().trim());//transpModFrete
        Assertions.assertEquals(1, ((BigDecimal) row.get("nfq720751")).intValue());//transpVserv
        Assertions.assertEquals(2, ((BigDecimal) row.get("nfq720752")).intValue());//transpVbcRet
        Assertions.assertEquals(3, ((BigDecimal) row.get("nfq720753")).intValue());//transpPicmsRet
        Assertions.assertEquals(4, ((BigDecimal) row.get("nfq720754")).intValue());//transpVicmsRet
        Assertions.assertEquals("5102", row.get("nfq720755").toString().trim());//transpCfop
        Assertions.assertEquals("SP", row.get("nfq720756").toString().trim());//transpCmunFg
        Assertions.assertEquals("DPB5386", row.get("nfq720757").toString().trim());//transpPlaca
        Assertions.assertEquals("MT", row.get("nfq720758").toString().trim());//transpUf
        Assertions.assertEquals("051680488", row.get("nfq720759").toString().trim());//transpRntc
        Assertions.assertEquals("1", row.get("nfq720760").toString().trim());//cobrNfat
        Assertions.assertEquals(2, ((BigDecimal) row.get("nfq720761")).intValue());//cobrVorig
        Assertions.assertEquals(3, ((BigDecimal) row.get("nfq720762")).intValue());//cobrVdesc
        Assertions.assertEquals(4, ((BigDecimal) row.get("nfq720763")).intValue());//cobrVliq
        Assertions.assertEquals("1", row.get("nfq720764").toString().trim());//infAdicFisco
        Assertions.assertEquals("infCpl", row.get("nfq720765").toString().trim());//InfAdicCpl
        Assertions.assertNull(row.get("nfq720766"));//exportaUfEmbarq - Coluna não utilizada na versão 4.00
        Assertions.assertNull(row.get("nfq720767"));//exportaXlocEmbarq - Coluna não utilizada na versão 4.00
        Assertions.assertNull(row.get("nfq720768"));//compraXnEmp - Coluna não utilizada na versão 4.00
        Assertions.assertEquals("273737", row.get("nfq720769").toString().trim());//xPed
        Assertions.assertEquals("teste", row.get("nfq720770").toString().trim());//xCont
        Assertions.assertEquals("2023-08-10T14:02:09-03:00", row.get("nfq721201").toString().trim());//ideDhEmi
        Assertions.assertEquals("2023-08-10T14:02:09-03:00", row.get("nfq721202").toString().trim());//ideDhSainEnt
        Assertions.assertEquals(2, ((BigDecimal) row.get("nfq721203")).intValue());//ideIdDest
        Assertions.assertEquals(0, ((BigDecimal) row.get("nfq721204")).intValue());//ideIndFinal
        Assertions.assertEquals(3, ((BigDecimal) row.get("nfq721205")).intValue());//ideIndPres
        Assertions.assertEquals(3, ((BigDecimal) row.get("nfq721240")).intValue());//icmsDeson
        Assertions.assertEquals(1, ((BigDecimal) row.get("nfq721241")).intValue());//issqnDcompet
        Assertions.assertEquals(1, ((BigDecimal) row.get("nfq721242")).intValue());//issqnVdeducao
        Assertions.assertEquals(2, ((BigDecimal) row.get("nfq721243")).intValue());//issqnVoutro
        Assertions.assertEquals(3, ((BigDecimal) row.get("nfq721244")).intValue());//issqnVdescIncond
        Assertions.assertEquals(4, ((BigDecimal) row.get("nfq721245")).intValue());//issqnVDescCond
        Assertions.assertEquals(5, ((BigDecimal) row.get("nfq721246")).intValue());//issqnVissRet
        Assertions.assertEquals(3, ((BigDecimal) row.get("nfq721247")).intValue());//issqnCregTrib
        Assertions.assertEquals(4, ((BigDecimal) row.get("nfq721310")).intValue());//icmsVfcpufDest
        Assertions.assertEquals(5, ((BigDecimal) row.get("nfq721311")).intValue());//icmsVicmsufDest
        Assertions.assertEquals(6, ((BigDecimal) row.get("nfq721312")).intValue());//icmsVicmsufDest
        Assertions.assertEquals(7, ((BigDecimal) row.get("nfq722411")).intValue());//icmsVfcp
        Assertions.assertEquals(1, ((BigDecimal) row.get("nfq722414")).intValue());//icmsVfcpst
        Assertions.assertEquals(2, ((BigDecimal) row.get("nfq722417")).intValue());//icmsVfcpstRet
        Assertions.assertEquals(8, ((BigDecimal) row.get("nfq722421")).intValue());//icmsVipiDevol
        Assertions.assertEquals("xNEmp", row.get("nfq722445").toString().trim());//nfq722445
    }
    @Test
    void validarTabelaKwnfe_itens() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72301 where niq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe_itens
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("niq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720653")).intValue());//nSequencia
        Assertions.assertEquals("1", row.get("niq720654").toString().trim());//prodCprod
        Assertions.assertEquals("1", row.get("niq720655").toString().trim());//prodCean
        Assertions.assertEquals("1", row.get("niq720656").toString().trim());//prodXprod
        Assertions.assertEquals("1", row.get("niq720657").toString().trim());//prodNcm
        Assertions.assertEquals("90", row.get("niq720658").toString().trim());//prodExtIpi
        Assertions.assertEquals(5102, ((BigDecimal)  row.get("niq720660")).intValue());//prodCfop
        Assertions.assertEquals("UN", row.get("niq720661").toString().trim());//prodUcom
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720662")).intValue());//prodQcom
        Assertions.assertEquals(20, ((BigDecimal) row.get("niq720663")).intValue());//prodVunCom
        Assertions.assertEquals("SEM GTIN", row.get("niq720665").toString().trim());//prodCeanTrib
        Assertions.assertEquals("UN", row.get("niq720666").toString().trim());//prodUtrib
        Assertions.assertEquals(10, ((BigDecimal) row.get("niq720667")).intValue());//prodQtrib
        Assertions.assertNull(row.get("niq720671"));//prodGenero - Coluna não utilizada na versão 4.00
        Assertions.assertEquals(70, ((BigDecimal) row.get("niq720672")).intValue());//prodVoutro
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720673")).intValue());//prodIndTot
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720674")).intValue());//icmsPmvast
        Assertions.assertEquals("32", row.get("niq720675").toString().trim());//nItemPed
        Assertions.assertEquals("4", row.get("niq720676").toString().trim());//icmsOrig
        Assertions.assertEquals("90", row.get("niq720677").toString().trim());//icmsCst
        Assertions.assertEquals("1", row.get("niq720678").toString().trim());//icmsModBc
        Assertions.assertEquals("1", row.get("niq720678").toString().trim());//icmsModBc
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq720679")).intValue());//icmspRedBc
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720680")).intValue());//icmsVbc
        Assertions.assertEquals(3, ((BigDecimal) row.get("niq720681")).intValue());//icmsPicms
        Assertions.assertEquals(4, ((BigDecimal) row.get("niq720682")).intValue());//icmsVicms
        Assertions.assertEquals("4", row.get("niq720683").toString().trim());//icmsModBcst
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq720684")).intValue());//icmspRedBcst
        Assertions.assertEquals(3, ((BigDecimal) row.get("niq720685")).intValue());//icmsVbcst
        Assertions.assertEquals(4, ((BigDecimal) row.get("niq720686")).intValue());//icmsPicmSst
        Assertions.assertEquals(5, ((BigDecimal) row.get("niq720687")).intValue());//icmsVicmSst
        Assertions.assertNull(row.get("niq720688"));//icmsMotDesIcms - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("niq720689"));//icmsVBcstRet - Campo não recuperado, Apath não consta no XML
        Assertions.assertEquals(3, ((BigDecimal) row.get("niq720690")).intValue());//icmsVicmSstRet
        Assertions.assertEquals(9, ((BigDecimal) row.get("niq720691")).intValue());//icmsPbcoP
        Assertions.assertEquals("DF", row.get("niq720692").toString().trim());//icmsUfst
        Assertions.assertEquals(7, ((BigDecimal) row.get("niq720693")).intValue());//icmsVbsctDest
        Assertions.assertEquals(8, ((BigDecimal) row.get("niq720694")).intValue());//icmsVicmSstDest
        Assertions.assertEquals(201, ((BigDecimal) row.get("niq720695")).intValue());//icmsCson
        Assertions.assertEquals(9, ((BigDecimal) row.get("niq720696")).intValue());//icmsPcredSn
        Assertions.assertEquals(10, ((BigDecimal) row.get("niq720697")).intValue());//icmsVcredIcmSsn
        Assertions.assertNull(row.get("niq720698"));//ipiCiEnq - Coluna não utilizada na versão 4.00
        Assertions.assertEquals("68337658000127", row.get("niq720699").toString().trim());//ipiCnpjProd
        Assertions.assertEquals("1", row.get("niq720700").toString().trim());//ipiCselo
        Assertions.assertEquals(15, ((BigDecimal) row.get("niq720728")).intValue());//prodVprod
        Assertions.assertEquals(30, ((BigDecimal) row.get("niq720729")).intValue());//prodVFrete
        Assertions.assertEquals(50, ((BigDecimal) row.get("niq720730")).intValue());//prodVseg
        Assertions.assertEquals(60, ((BigDecimal) row.get("niq720731")).intValue());//prodVdesc
        Assertions.assertEquals("AMOSTRA", row.get("niq720769").toString().trim());//prodXped
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq720771")).intValue());//impostoQselo
        Assertions.assertEquals("3", row.get("niq720772").toString().trim());//impostoCenq
        Assertions.assertEquals("50", row.get("niq720773").toString().trim());//impostoCst
        Assertions.assertEquals(4, ((BigDecimal) row.get("niq720774")).intValue());//impostoVbc
        Assertions.assertEquals(6, ((BigDecimal) row.get("niq720775")).intValue());//impostoQunid
        Assertions.assertEquals(7, ((BigDecimal) row.get("niq720776")).intValue());//impostoVunid
        Assertions.assertEquals(5, ((BigDecimal) row.get("niq720777")).intValue());//impostoPipi
        Assertions.assertEquals(8, ((BigDecimal) row.get("niq720778")).intValue());//impostoVipi
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720779")).intValue());// Não consta na planilha de mapeamento
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq720780")).intValue());//impostoVdespAdu
        Assertions.assertEquals(3, ((BigDecimal) row.get("niq720781")).intValue());//impostoVii
        Assertions.assertEquals(7, ((BigDecimal) row.get("niq720782")).intValue());//impostoViof
        Assertions.assertEquals("02", row.get("niq720783").toString().trim());//PISAliqCst
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720784")).intValue());//PISAliqVbc
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq720785")).intValue());//PISAliqPpis
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720786")).intValue());//PISAliqQbcProd
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq720787")).intValue());//PISAliqValiqProd
        Assertions.assertEquals(3, ((BigDecimal) row.get("niq720788")).intValue());//PISAliqVpis
        Assertions.assertEquals("02", row.get("niq720789").toString().trim());//confinsCst
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720790")).intValue());//confinsVbc
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq720791")).intValue());//confinsPcofins
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720792")).intValue());//confinsQbcProd
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq720793")).intValue());//confinsValiqProd
        Assertions.assertEquals(3, ((BigDecimal) row.get("niq720794")).intValue());//confinsVCofins
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq720795")).intValue());//issqnVbc
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq720796")).intValue());//issqnValiq
        Assertions.assertEquals(3, ((BigDecimal) row.get("niq720797")).intValue());//issqnVissqn
        Assertions.assertEquals(4, ((BigDecimal) row.get("niq720798")).intValue());//issqnCmunFg
        Assertions.assertEquals("5", row.get("niq720799").toString().trim());//issqnClistServ
        Assertions.assertNull(row.get("niq720800"));//issqnCsitTrib - Campo não recuperado, Apath não consta no XML
        Assertions.assertEquals("1", row.get("niq720801").toString().trim());//infAdProd
        Assertions.assertEquals(12345, ((BigDecimal) row.get("niq721221")).intValue());//prodNrecopi
        Assertions.assertEquals(1, ((BigDecimal)  row.get("niq721222")).intValue());//impostoVtotTrib
        Assertions.assertNull(row.get("niq721223"));//icmsVicmsDeson - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("niq721224"));//icmsVicmsOp - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("niq721225"));//icmsPDif - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("niq721226"));//icmsVicmsDif - Campo não recuperado, Apath não consta no XML
        Assertions.assertEquals(6, ((BigDecimal) row.get("niq721227")).intValue());//issqnVDeducao
        Assertions.assertEquals(7, ((BigDecimal) row.get("niq721228")).intValue());//issqnVOutro
        Assertions.assertEquals(8, ((BigDecimal) row.get("niq721229")).intValue());//issqnVDescIncond
        Assertions.assertEquals(9, ((BigDecimal) row.get("niq721230")).intValue());//issqnVDescCond
        Assertions.assertEquals(10, ((BigDecimal) row.get("niq721231")).intValue());//issqnVissRet
        Assertions.assertEquals(7, ((BigDecimal) row.get("niq721232")).intValue());//issqnIndIss
        Assertions.assertEquals("1", row.get("niq721233").toString().trim());//issqnCServico
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq721234")).intValue());//issqnCMun
        Assertions.assertEquals(3, ((BigDecimal) row.get("niq721235")).intValue());//issqnCPais
        Assertions.assertEquals("4", row.get("niq721236").toString().trim());//issqnNProcesso
        Assertions.assertEquals("1", row.get("niq721237").toString().trim());//issqnIndIncentivo
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq721238")).intValue());//impostoDevolPDevol
        Assertions.assertNull(row.get("niq721239"));//impostoDevolVIpiDevol - Campo não recuperado corretamente
        Assertions.assertEquals("1", row.get("niq721268").toString().trim());//prodNve
        Assertions.assertEquals("01F70AF-10BF", row.get("niq721269").toString().trim());//prodNfci
        Assertions.assertNull(row.get("niq721270"));//prodNve2 - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("niq721271"));//prodNve3 - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("niq721272"));//prodNve4 - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("niq721273"));//prodNve5 - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("niq721274"));//prodNve6 - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("niq721275"));//prodNve7 - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("niq721276"));//prodNve8 - Campo não recuperado, Apath não consta no XML
        Assertions.assertEquals("0104900", row.get("niq721304").toString().trim());//prodCest
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq721305")).intValue());//vBCUFDest
        Assertions.assertEquals(3, ((BigDecimal) row.get("niq721306")).intValue());//pFCPUFDest
        Assertions.assertEquals(5, ((BigDecimal) row.get("niq721307")).intValue());//pICMSUFDest
        Assertions.assertEquals(4, ((BigDecimal) row.get("niq721308")).intValue());//pICMSInter
        Assertions.assertEquals(6, ((BigDecimal) row.get("niq721309")).intValue());//pICMSInterPart
        Assertions.assertEquals(7, ((BigDecimal) row.get("niq721310")).intValue());//vFCPUFDest
        Assertions.assertEquals(8, ((BigDecimal) row.get("niq721311")).intValue());//vICMSUFDest
        Assertions.assertEquals(9, ((BigDecimal) row.get("niq721312")).intValue());//vICMSUFRemet
        Assertions.assertEquals(5, ((BigDecimal) row.get("niq722410")).intValue());//vFCPUFDest
        Assertions.assertEquals(6, ((BigDecimal) row.get("niq722411")).intValue());//vICMSUFDest
        Assertions.assertEquals(6, ((BigDecimal) row.get("niq722412")).intValue());//vICMSUFRemet
        Assertions.assertEquals(7, ((BigDecimal) row.get("niq722413")).intValue());//icmsPfcpst
        Assertions.assertEquals(8, ((BigDecimal) row.get("niq722414")).intValue());//icmsVfcpst
        Assertions.assertNull(row.get("niq722415"));//icmsVbcfcp - Campo não recuperado, Apath não consta no XML
        Assertions.assertEquals(5, ((BigDecimal) row.get("niq722416")).intValue());//icmsPfcpstRet
        Assertions.assertEquals(6, ((BigDecimal) row.get("niq722417")).intValue());//icmsVfdcpstRet
        Assertions.assertEquals(1, ((BigDecimal) row.get("niq722418")).intValue());//icmsPst
        Assertions.assertEquals(4, ((BigDecimal) row.get("niq722419")).intValue());//VbcfcpStRet
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq722420")).intValue());//VbcfcpUfRet
        Assertions.assertEquals("S", row.get("niq722429").toString().trim());//prodIndEscala
        Assertions.assertEquals("44676138000180", row.get("niq722430").toString().trim());//prodCnpjFab
        Assertions.assertEquals("1", row.get("niq722431").toString().trim());//prodCBenef
        Assertions.assertEquals(9, ((BigDecimal) row.get("niq722435")).intValue());//pRedBcefet
        Assertions.assertEquals(10, ((BigDecimal) row.get("niq722436")).intValue());//vBceFet
        Assertions.assertEquals(11, ((BigDecimal) row.get("niq722437")).intValue());//pIcmsEfet
        Assertions.assertEquals(12, ((BigDecimal) row.get("niq722438")).intValue());//vIcmsEfet
        Assertions.assertEquals(2, ((BigDecimal) row.get("niq722439")).intValue());//vIcmsSubstituto
        Assertions.assertNull(row.get("niq720802"));// Não consta na planilha de mapeamento
    }
    @Test
    void validarTabelaKwnfe_enderecos_dest() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72302 where neq720652 like '%NFe35230856493877000116550010002073651002155823%' and neq720803 = 'dest'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("neq720652").toString().trim());//id
        Assertions.assertEquals("dest", row.get("neq720803").toString().trim());//cTipo
        Assertions.assertEquals("08062665000565", row.get("neq720804").toString().trim());//cnpj
        Assertions.assertNull(row.get("neq720805"));//cpf - Campo não recuperado, Apath não consta no XML
        Assertions.assertEquals("VETTA", row.get("neq720806").toString().trim());//xNome
        Assertions.assertNull(row.get("neq720807"));//xFant
        Assertions.assertEquals("Rodovia", row.get("neq720808").toString().trim());//xLgr
        Assertions.assertEquals("S/N", row.get("neq720809").toString().trim());//nro
        Assertions.assertEquals("s/c", row.get("neq720810").toString().trim());//xCpl
        Assertions.assertEquals("Jardim", row.get("neq720811").toString().trim());//xBairro
        Assertions.assertEquals("3538709", row.get("neq720812").toString().trim());//cMun
        Assertions.assertEquals("Piracicaba", row.get("neq720813").toString().trim());//xMun
        Assertions.assertEquals("CE", row.get("neq720814").toString().trim());//uf
        Assertions.assertEquals(13412000, ((BigDecimal) row.get("neq720815")).intValue());//cep
        Assertions.assertEquals(1058, ((BigDecimal) row.get("neq720816")).intValue());//cPais
        Assertions.assertEquals("Brasil", row.get("neq720817").toString().trim());//xPais
        Assertions.assertNull(row.get("neq720818"));// Não consta na planilha de mapeamento
        Assertions.assertEquals("5037646870028", row.get("neq720819").toString().trim());//ie
        Assertions.assertEquals("12345678", row.get("neq720820").toString().trim());//isuf
        Assertions.assertNull(row.get("neq720821"));//iest
        Assertions.assertEquals("15657", row.get("neq720822").toString().trim());//im
        Assertions.assertEquals("1124363838", row.get("neq720823").toString().trim());//fone
        Assertions.assertEquals("string@email.com", row.get("neq720824").toString().trim());//email
        Assertions.assertNull(row.get("neq720825"));//cnae
        Assertions.assertNull(row.get("neq720826"));//crt
        Assertions.assertEquals("0", row.get("neq721206").toString().trim());//idEstrangeiro
        Assertions.assertEquals(2, ((BigDecimal) row.get("neq721207")).intValue());//indIEDest
    }
    @Test
    void validarTabelaKwnfe_enderecos_emit() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72302 where neq720652 like '%NFe35230856493877000116550010002073651002155823%' and neq720803 = 'emit'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("neq720652").toString().trim());//id
        Assertions.assertEquals("emit", row.get("neq720803").toString().trim());//cTipo
        Assertions.assertEquals("56493877000116", row.get("neq720804").toString().trim());//cnpj
        Assertions.assertNull(row.get("neq720805"));//cpf - Campo não recuperado, Apath não consta no XML
        Assertions.assertEquals("RONCOLI NOME", row.get("neq720806").toString().trim());//xNome
        Assertions.assertEquals("RONCOLI", row.get("neq720807").toString().trim());//xFant
        Assertions.assertEquals("aV 11", row.get("neq720808").toString().trim());//xLgr
        Assertions.assertEquals("2", row.get("neq720809").toString().trim());//nro
        Assertions.assertEquals("CASA 3", row.get("neq720810").toString().trim());//xCpl
        Assertions.assertEquals("CENTRO", row.get("neq720811").toString().trim());//xBairro
        Assertions.assertEquals("3543907", row.get("neq720812").toString().trim());//cMun
        Assertions.assertEquals("RIO CLARO", row.get("neq720813").toString().trim());//xMun
        Assertions.assertEquals("SP", row.get("neq720814").toString().trim());//uf
        Assertions.assertEquals(13500350, ((BigDecimal) row.get("neq720815")).intValue());//cep
        Assertions.assertEquals(1058, ((BigDecimal) row.get("neq720816")).intValue());//cPais
        Assertions.assertEquals("Brasil", row.get("neq720817").toString().trim());//xPais
        Assertions.assertNull(row.get("neq720818"));// Não consta na planilha de mapeamento
        Assertions.assertEquals("587032758111", row.get("neq720819").toString().trim());//ie
        Assertions.assertNull(row.get("neq720820"));//isuf
        Assertions.assertEquals("587032758111", row.get("neq720821").toString().trim());//iest
        Assertions.assertEquals("587032758112", row.get("neq720822").toString().trim());//im
        Assertions.assertEquals("1935222300", row.get("neq720823").toString().trim());//fone
        Assertions.assertNull(row.get("neq720824"));//email
        Assertions.assertEquals("12", row.get("neq720825").toString().trim());//cnae
        Assertions.assertEquals(3, ((BigDecimal)  row.get("neq720826")).intValue());//crt
        Assertions.assertNull(row.get("neq721206"));//idEstrangeiro
        Assertions.assertNull(row.get("neq721207"));//indIEDest
    }
    @Test
    void validarTabelaKwnfe_enderecos_entrega() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72302 where neq720652 like '%NFe35230856493877000116550010002073651002155823%' and neq720803 = 'entrega'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("neq720652").toString().trim());//id
        Assertions.assertEquals("entrega", row.get("neq720803").toString().trim());//cTipo
        Assertions.assertEquals("56493877000116", row.get("neq720804").toString().trim());//cnpj
        Assertions.assertNull(row.get("neq720805"));//cpf - Campo não recuperado, Apath não consta no XML
        Assertions.assertEquals("entrega xnome", row.get("neq720806").toString().trim());//xNome
        Assertions.assertNull(row.get("neq720807"));//xFant
        Assertions.assertEquals("entrega xlgr", row.get("neq720808").toString().trim());//xLgr
        Assertions.assertEquals("02", row.get("neq720809").toString().trim());//nro
        Assertions.assertEquals("entrega xcpl", row.get("neq720810").toString().trim());//xCpl
        Assertions.assertEquals("entrega xbairro", row.get("neq720811").toString().trim());//xBairro
        Assertions.assertEquals("3543907", row.get("neq720812").toString().trim());//cMun
        Assertions.assertEquals("entrega xmun", row.get("neq720813").toString().trim());//xMun
        Assertions.assertEquals("DF", row.get("neq720814").toString().trim());//uf
        Assertions.assertEquals(13500350, ((BigDecimal) row.get("neq720815")).intValue());//cep
        Assertions.assertEquals(1058, ((BigDecimal) row.get("neq720816")).intValue());//cPais
        Assertions.assertEquals("Brasil", row.get("neq720817").toString().trim());//xPais
        Assertions.assertNull(row.get("neq720818"));// Não consta na planilha de mapeamento
        Assertions.assertEquals("587032758111", row.get("neq720819").toString().trim());//ie
        Assertions.assertNull(row.get("neq720820"));//isuf
        Assertions.assertNull(row.get("neq720821"));//iest
        Assertions.assertNull(row.get("neq720822"));//im
        Assertions.assertEquals("1935222300", row.get("neq720823").toString().trim());//fone
        Assertions.assertEquals("entrega@email.com", row.get("neq720824").toString().trim());//email
        Assertions.assertNull(row.get("neq720825"));//cnae
        Assertions.assertNull(row.get("neq720826"));//crt
        Assertions.assertNull(row.get("neq721206"));//idEstrangeiro
        Assertions.assertNull(row.get("neq721207"));//indIEDest
    }
    @Test
    void validarTabelaKwnfe_enderecos_retirada() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72302 where neq720652 like '%NFe35230856493877000116550010002073651002155823%' and neq720803 = 'retirada'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("neq720652").toString().trim());//id
        Assertions.assertEquals("retirada", row.get("neq720803").toString().trim());//cTipo
        Assertions.assertEquals("56493877000116", row.get("neq720804").toString().trim());//cnpj
        Assertions.assertNull(row.get("neq720805"));//cpf - Campo não recuperado, Apath não consta no XML
        Assertions.assertEquals("retirada xNome", row.get("neq720806").toString().trim());//xNome
        Assertions.assertNull(row.get("neq720807"));//xFant
        Assertions.assertEquals("retirada xLgr", row.get("neq720808").toString().trim());//xLgr
        Assertions.assertEquals("01", row.get("neq720809").toString().trim());//nro
        Assertions.assertEquals("retirada xCpl", row.get("neq720810").toString().trim());//xCpl
        Assertions.assertEquals("retirada xBairro", row.get("neq720811").toString().trim());//xBairro
        Assertions.assertEquals("3543907", row.get("neq720812").toString().trim());//cMun
        Assertions.assertEquals("retirada xMun", row.get("neq720813").toString().trim());//xMun
        Assertions.assertEquals("AL", row.get("neq720814").toString().trim());//uf
        Assertions.assertEquals(13500350, ((BigDecimal) row.get("neq720815")).intValue());//cep
        Assertions.assertEquals(1058, ((BigDecimal) row.get("neq720816")).intValue());//cPais
        Assertions.assertEquals("Brasil", row.get("neq720817").toString().trim());//xPais
        Assertions.assertNull(row.get("neq720818"));// Não consta na planilha de mapeamento
        Assertions.assertEquals("587032758111", row.get("neq720819").toString().trim());//ie
        Assertions.assertNull(row.get("neq720820"));//isuf
        Assertions.assertNull(row.get("neq720821"));//iest
        Assertions.assertNull(row.get("neq720822"));//im
        Assertions.assertEquals("1935222300", row.get("neq720823").toString().trim());//fone
        Assertions.assertEquals("retirada@email.com", row.get("neq720824").toString().trim());//email
        Assertions.assertNull(row.get("neq720825"));//cnae
        Assertions.assertNull(row.get("neq720826"));//crt
        Assertions.assertNull(row.get("neq721206"));//idEstrangeiro
        Assertions.assertNull(row.get("neq721207"));//indIEDest
    }
    @Test
    void validarTabelaKwnfe_enderecos_transporta() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72302 where neq720652 like '%NFe35230856493877000116550010002073651002155823%' and neq720803 = 'transporta'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("neq720652").toString().trim());//id
        Assertions.assertEquals("transporta", row.get("neq720803").toString().trim());//cTipo
        Assertions.assertEquals("68337658000127", row.get("neq720804").toString().trim());//cnpj
        Assertions.assertNull(row.get("neq720805"));//cpf - Campo não recuperado, Apath não consta no XML
        Assertions.assertEquals("transporta xNome", row.get("neq720806").toString().trim());//xNome
        Assertions.assertNull(row.get("neq720807"));//xFant
        Assertions.assertNull(row.get("neq720808"));//xLgr
        Assertions.assertNull(row.get("neq720809"));//nro
        Assertions.assertNull(row.get("neq720810"));//xCpl
        Assertions.assertNull(row.get("neq720811"));//xBairro
        Assertions.assertNull(row.get("neq720812"));//cMun
        Assertions.assertEquals("transporta xMun", row.get("neq720813").toString().trim());//xMun
        Assertions.assertEquals("ES", row.get("neq720814").toString().trim());//uf
        Assertions.assertNull(row.get("neq720815"));//cep
        Assertions.assertNull(row.get("neq720816"));//cPais
        Assertions.assertNull(row.get("neq720817"));//xPais
        Assertions.assertEquals("transporta xEnder", row.get("neq720818").toString().trim());// Não consta na planilha de mapeamento
        Assertions.assertEquals("123456789", row.get("neq720819").toString().trim());//ie
        Assertions.assertNull(row.get("neq720820"));//isuf
        Assertions.assertNull(row.get("neq720821"));//iest
        Assertions.assertNull(row.get("neq720822"));//im
        Assertions.assertNull(row.get("neq720823"));//fone
        Assertions.assertNull(row.get("neq720824"));//email
        Assertions.assertNull(row.get("neq720825"));//cnae
        Assertions.assertNull(row.get("neq720826"));//crt
        Assertions.assertNull(row.get("neq721206"));//idEstrangeiro
        Assertions.assertNull(row.get("neq721207"));//indIEDest
    }
    @Test
    void validarTabelaKwnfe_referenciadas() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72303 where nrq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nrq720652").toString().trim());//id
        Assertions.assertEquals("01", row.get("nrq720707").toString().trim());//ideserie
        Assertions.assertNull(row.get("nrq720805"));//ideCpf
        Assertions.assertEquals(1, ((BigDecimal) row.get("nrq720849")).intValue());//nSequencia
        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nrq720850").toString().trim());//ideRefNfe
        Assertions.assertEquals("33", row.get("nrq720851").toString().trim());//ideCuf
        Assertions.assertEquals("1", row.get("nrq720852").toString().trim());//ideAmm
        Assertions.assertEquals("05054903000179", row.get("nrq720853").toString().trim());//ideCnpj
        Assertions.assertEquals("2D", row.get("nrq720855").toString().trim());//ideMod
        Assertions.assertEquals(22, ((BigDecimal) row.get("nrq720857")).intValue());//ideNnf
        Assertions.assertEquals("123", row.get("nrq720858").toString().trim());//ideIe
        Assertions.assertEquals("CTe29200606203406000662570110000086721008919921", row.get("nrq720859").toString().trim());//ideRefCte
        Assertions.assertEquals(1, ((BigDecimal) row.get("nrq720860")).intValue());//ideNEcf
        Assertions.assertEquals(111, ((BigDecimal) row.get("nrq720861")).intValue());//ideNcoo
    }
    @Test
    void validarTabelaKwnfe_avulsa() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72304 where naq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("naq720652").toString().trim());//id
        Assertions.assertEquals("2023-04-10", row.get("naq720709").toString().trim());//ideDEmi
        Assertions.assertEquals("CE", row.get("naq720814").toString().trim());//ideUF
        Assertions.assertEquals("05054903000179", row.get("naq720828").toString().trim());//ideCnpj
        Assertions.assertEquals("SECRETARIA", row.get("naq720829").toString().trim());//ideXOrgao
        Assertions.assertEquals("Portal", row.get("naq720830").toString().trim());//ideMatr
        Assertions.assertEquals("xAgente", row.get("naq720831").toString().trim());//ideXAgente
        Assertions.assertEquals("fone", row.get("naq720832").toString().trim());//ideFone
        Assertions.assertEquals("nDAR", row.get("naq720834").toString().trim());//ideNDar
        Assertions.assertEquals(2, ((BigDecimal) row.get("naq720836")).intValue());//ideVDar
        Assertions.assertEquals("repEmi", row.get("naq720837").toString().trim());//ideRepEmi
        Assertions.assertEquals("0", row.get("naq720838").toString().trim());//ideDPag
    }
    @Test
    void validarTabelaKwnfe_volumes() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72305 where nvq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nvq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("nvq720849")).intValue());//nSequencia
        Assertions.assertEquals(1, ((BigDecimal) row.get("nvq720864")).intValue());//volQVol
        Assertions.assertEquals("2", row.get("nvq720865").toString().trim());//volEsp
        Assertions.assertEquals("3", row.get("nvq720866").toString().trim());//volMarca
        Assertions.assertEquals("4", row.get("nvq720867").toString().trim());//volNVol
        Assertions.assertEquals(5, ((BigDecimal) row.get("nvq720868")).intValue());//volPesoL
        Assertions.assertEquals(6, ((BigDecimal) row.get("nvq720869")).intValue());//volPesoB
    }
    @Test
    void validarTabelaKwnfe_dados_reboque() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72306 where ndq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("ndq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("ndq720849")).intValue());//nSequencia
        Assertions.assertEquals("AKE5076", row.get("ndq720757").toString().trim());//veicTranspPlaca
        Assertions.assertEquals("GO", row.get("ndq720758").toString().trim());//veicTranspUf
        Assertions.assertNull(row.get("ndq720843"));// Não consta na planilha de mapeamento
        Assertions.assertNull(row.get("ndq720844"));// Não consta na planilha de mapeamento
        Assertions.assertEquals("051680489", row.get("ndq720759").toString().trim());//veicTranspRntc
        Assertions.assertNull(row.get("ndq720846"));//veicTranspVagao - Campo não recuperado, Apath não consta no XML
        Assertions.assertNull(row.get("ndq720847"));//veicTranspBalsa - Campo não recuperado, Apath não consta no XML
    }
    @Test
    void validarTabelaKwnfe_lacres() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72307 where nlq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row1 = result.get(0);
        Map<String, Object> row2 = result.get(1);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row1.get("nlq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row1.get("nlq720871")).intValue());//nSequencia
        Assertions.assertEquals(1, ((BigDecimal) row1.get("nlq720872")).intValue());//nSequenciaFk
        Assertions.assertEquals("7", row1.get("nlq720873").toString().trim());//nLacre

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row2.get("nlq720652").toString().trim());//id
        Assertions.assertEquals(2, ((BigDecimal) row2.get("nlq720871")).intValue());//nSequencia
        Assertions.assertEquals(1, ((BigDecimal) row2.get("nlq720872")).intValue());//nSequenciaFk
        Assertions.assertEquals("8", row2.get("nlq720873").toString().trim());//nLacre
    }
    @Test
    void validarTabelaKwnfe_faturas() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72308 where nuq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nuq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("nuq720849")).intValue());//nSequencia
        Assertions.assertEquals("1", row.get("nuq720876").toString().trim());//dupNDup
        Assertions.assertEquals("2023-04-05", row.get("nuq720877").toString().trim());//dupDVenc
        Assertions.assertEquals(3, ((BigDecimal) row.get("nuq720878")).intValue());//dupVDup
    }
    @Test
    void validarTabelaKwnfe_observacao() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72309 where noq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row1 = result.get(0);
        Map<String, Object> row2 = result.get(1);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row1.get("noq720652").toString().trim());//id
        Assertions.assertEquals("c", row1.get("noq720880").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row1.get("noq720849")).intValue());//nSequencia
        Assertions.assertEquals("xCampo1", row1.get("noq720882").toString().trim());//dupNDup
        Assertions.assertEquals("xTexto1", row1.get("noq720883").toString().trim());//dupDVenc

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row2.get("noq720652").toString().trim());//id
        Assertions.assertEquals("f", row2.get("noq720880").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row2.get("noq720849")).intValue());//nSequencia
        Assertions.assertEquals("xCampo2", row2.get("noq720882").toString().trim());//dupNDup
        Assertions.assertEquals("xTexto2", row2.get("noq720883").toString().trim());//dupDVenc
    }
    @Test
    void validarTabelaKwnfe_processos() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72310 where npq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("npq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("npq720849")).intValue());//nSequencia
        Assertions.assertEquals("50043960620174047205", row.get("npq720886").toString().trim());//nProc
        Assertions.assertEquals("1", row.get("npq720887").toString().trim());//indProc
    }
    @Test
    void validarTabelaKwnfe_di_adicoes() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72311 where ncq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("ncq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("ncq720889")).intValue());//nSequencia
        Assertions.assertEquals("1", row.get("ncq720850").toString().trim());//nSequenciaAdi
        Assertions.assertEquals(1, ((BigDecimal) row.get("ncq720900")).intValue());//nSequenciaSeqAdic
        Assertions.assertEquals(1, ((BigDecimal) row.get("ncq720901")).intValue());//nAdicao
        Assertions.assertEquals("A DEFINIR", row.get("ncq720902").toString().trim());//cFabricante
        Assertions.assertEquals(2, ((BigDecimal) row.get("ncq720903")).intValue());//vDescDI
        Assertions.assertEquals(2, ((BigDecimal) row.get("ncq721213")).intValue());//nDraw
    }
    @Test
    void validarTabelaKwnfe_itens_di() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72312 where nbq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nbq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("nbq720889")).intValue());//nSequencia
        Assertions.assertEquals(1, ((BigDecimal) row.get("nbq720849")).intValue());//nSequenciaDi
        Assertions.assertEquals("2315699664", row.get("nbq720891").toString().trim());//nDI
        Assertions.assertEquals("2023-08-11", row.get("nbq720892").toString().trim());//dDI
        Assertions.assertEquals("Sao Paulo", row.get("nbq720893").toString().trim());//xLocDesemb
        Assertions.assertEquals("SP", row.get("nbq720894").toString().trim());//UFDesemb
        Assertions.assertEquals("2023-08-11", row.get("nbq720895").toString().trim());//dDesemb
        Assertions.assertEquals("SCL", row.get("nbq720896").toString().trim());//cExportador
        Assertions.assertEquals(13, ((BigDecimal) row.get("nbq721208")).intValue());//tpViaTransp
        Assertions.assertEquals(2470.0, ((BigDecimal) row.get("nbq721209")).intValue());//vAFRMM - Campo não recuperado corretamente: '2470.27'
        Assertions.assertEquals(2, ((BigDecimal) row.get("nbq721210")).intValue());//tpIntermedio
        Assertions.assertEquals("53425120000105", row.get("nbq721211").toString().trim());
        Assertions.assertEquals("SC", row.get("nbq721212").toString().trim());//cExportador
    }
    @Test
    void validarTabelaKwnfe_itens_veiculos() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72313 where ngq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("ngq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("ngq720653")).intValue());//nSequencia
        Assertions.assertEquals(3, ((BigDecimal) row.get("ngq720906")).intValue());//tpOp
        Assertions.assertEquals("1236", row.get("ngq720907").toString().trim());//chassi
        Assertions.assertEquals("cCor", row.get("ngq720908").toString().trim());//cCor
        Assertions.assertEquals("xCor", row.get("ngq720909").toString().trim());//xCor
        Assertions.assertEquals("pot", row.get("ngq720910").toString().trim());//pot
        Assertions.assertNull(row.get("ngq720911"));//cm - Coluna não utilizada na versão 4.00
        Assertions.assertEquals("cilin", row.get("ngq720912").toString().trim());//cilin
        Assertions.assertEquals("1", row.get("ngq720913").toString().trim());//pesoL
        Assertions.assertEquals("2", row.get("ngq720914").toString().trim());//pesoB
        Assertions.assertEquals("321", row.get("ngq720915").toString().trim());//nSerie
        Assertions.assertEquals("st", row.get("ngq720916").toString().trim());//tpComb
        Assertions.assertEquals("124", row.get("ngq720917").toString().trim());//nMotor
        Assertions.assertNull(row.get("ngq720918"));//cmkg - Coluna não utilizada na versão 4.00
        Assertions.assertEquals("012010", row.get("ngq720919").toString().trim());//cmkg
        Assertions.assertEquals("dist", row.get("ngq720920").toString().trim());//cmt
        Assertions.assertNull(row.get("ngq720921"));//renavam - Coluna não utilizada na versão 4.00
        Assertions.assertEquals(2023, ((BigDecimal) row.get("ngq720922")).intValue());//anoMod
        Assertions.assertEquals(2023, ((BigDecimal) row.get("ngq720923")).intValue());//anoFab
        Assertions.assertEquals("s", row.get("ngq720924").toString().trim());//tpPint
        Assertions.assertEquals(3, ((BigDecimal) row.get("ngq720925")).intValue());//anoMod
        Assertions.assertEquals(4, ((BigDecimal) row.get("ngq720926")).intValue());//anoFab
        Assertions.assertEquals("R", row.get("ngq720927").toString().trim());//vin
        Assertions.assertEquals(3, ((BigDecimal) row.get("ngq720928")).intValue());//condVeic
        Assertions.assertEquals(1, ((BigDecimal) row.get("ngq720929")).intValue());//cMod
        Assertions.assertEquals("st", row.get("ngq720930").toString().trim());//cCorDenatram
        Assertions.assertEquals(3, ((BigDecimal) row.get("ngq720931")).intValue());//lota
        Assertions.assertEquals(1, ((BigDecimal) row.get("ngq720932")).intValue());//tpRest
    }
    @Test
    void validarTabelaKwnfe_inf_lotes() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72314 where nhq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nhq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("nhq720653")).intValue());//nSequencia
        Assertions.assertEquals(1, ((BigDecimal) row.get("nhq720849")).intValue());//nItem
        Assertions.assertNull(row.get("nhq720935"));//nLote
        Assertions.assertNull(row.get("nhq720879"));//qLote
        Assertions.assertNull(row.get("nhq720835"));//dFab
        Assertions.assertNull(row.get("nhq720836"));//dVal
        Assertions.assertEquals(50, ((BigDecimal) row.get("nhq720937")).intValue());//vPmc
        Assertions.assertEquals("1234", row.get("nhq722404").toString().trim());// Não consta na planilha de mapeamento
        Assertions.assertEquals("teste", row.get("nhq722440").toString().trim());// Não consta na planilha de mapeamento
    }

    @Test
    void validarTabelaKwnfe_inf_armas() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72315 where njq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("njq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("njq720849")).intValue());//nSequencia
        Assertions.assertEquals(1, ((BigDecimal) row.get("njq720653")).intValue());//nItem
        Assertions.assertEquals(1, ((BigDecimal) row.get("njq720940")).intValue());//tpArma
        Assertions.assertNull(row.get("njq720941"));// Não consta na planilha de mapeamento
        Assertions.assertNull(row.get("njq720942"));// Não consta na planilha de mapeamento
        Assertions.assertEquals("descr", row.get("njq720943").toString().trim());//descr
        Assertions.assertEquals("012", row.get("njq721218").toString().trim());//nSerie
        Assertions.assertEquals("5678", row.get("njq721219").toString().trim());//nCano
    }
    @Test
    void validarTabelaKwnfe_inf_combustiveis() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72316 where nkq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nkq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("nkq720653")).intValue());//nSequencia
        Assertions.assertEquals(2, ((BigDecimal) row.get("nkq720946")).intValue());//cProdANP
        Assertions.assertEquals(5, ((BigDecimal) row.get("nkq720947")).intValue());//codif
        Assertions.assertEquals(6, ((BigDecimal) row.get("nkq720948")).intValue());//qTemp
        Assertions.assertNull(row.get("nkq720949"));//ufCons
        Assertions.assertEquals(1, ((BigDecimal) row.get("nkq720950")).intValue());//qBCProd
        Assertions.assertEquals(2, ((BigDecimal) row.get("nkq720951")).intValue());//vAliqProd
        Assertions.assertEquals(3, ((BigDecimal) row.get("nkq720952")).intValue());//vCide
        Assertions.assertNull(row.get("nkq720953"));//vBcicms - Coluna não utilizada na versão 4.00
        Assertions.assertNull(row.get("nkq720954"));//vBcicmSst - Coluna não utilizada na versão 4.00
        Assertions.assertNull(row.get("nkq720955"));//vIcmSst - Coluna não utilizada na versão 4.00
        Assertions.assertNull(row.get("nkq720956"));//vBcIcmsStDest - Coluna não utilizada na versão 4.00
        Assertions.assertNull(row.get("nkq720957"));//vIcmsStDest - Coluna não utilizada na versão 4.00
        Assertions.assertNull(row.get("nkq720958"));//vBcIcmsStCons - Coluna não utilizada na versão 4.00
        Assertions.assertNull(row.get("nkq720959"));//vIcmsSTCons - Coluna não utilizada na versão 4.00
        Assertions.assertNull(row.get("nkq721220"));//pMixGn - Coluna não utilizada na versão 4.00
        Assertions.assertEquals("descANP", row.get("nkq722405").toString().trim());//descAnp
        Assertions.assertEquals(1, ((BigDecimal) row.get("nkq722406")).intValue());//pGlp
        Assertions.assertEquals(2, ((BigDecimal) row.get("nkq722407")).intValue());//pGnn
        Assertions.assertEquals(3, ((BigDecimal) row.get("nkq722408")).intValue());//pGni
        Assertions.assertEquals(4, ((BigDecimal) row.get("nkq722409")).intValue());//vPart
        Assertions.assertEquals(1, ((BigDecimal) row.get("nkq722424")).intValue());//nBico
        Assertions.assertEquals(2, ((BigDecimal) row.get("nkq722425")).intValue());//nBomba
        Assertions.assertEquals(3, ((BigDecimal) row.get("nkq722426")).intValue());//nTanque
        Assertions.assertEquals(4, ((BigDecimal) row.get("nkq722427")).intValue());//vEncIni
        Assertions.assertEquals(5, ((BigDecimal) row.get("nkq722428")).intValue());//vEncFin
    }
    @Test
    void validarTabelaKwnfe_infresptec() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72336 where nfq72c100 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nfq72c100").toString().trim());//id
        Assertions.assertEquals("30970854000161", row.get("nfq72c154").toString().trim());//infRespTecCnpj
        Assertions.assertEquals("xContato", row.get("nfq720806").toString().trim());//infRespTecXContato
        Assertions.assertEquals("email@EMAIL.COM", row.get("nfq720824").toString().trim());//infRespTecEmail
        Assertions.assertEquals("03836715156", row.get("nfq720823").toString().trim());//infRespTecFone
        Assertions.assertEquals("123", row.get("nfq72c385").toString().trim());//infRespTecIdCsrt
        Assertions.assertEquals("ZGFyZWRhcmVkYXJlZGFyZWRhcmU=", row.get("nfq72c386").toString().trim());//infRespTecHashCsrt
    }
    @Test
    void validarTabelaKwnfe_infcpl() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72300a where nfq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nfq720652").toString().trim());//id
        Assertions.assertEquals(0, ((BigDecimal) row.get("nfq720653")).intValue());//nSequencia
        Assertions.assertEquals("infCpl", row.get("nfq721200").toString().trim());//infAdicInfCpl
    }
    @Test
    void validarTabelaKwnfe_autxml() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72322 where n3q720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("n3q720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q720849")).intValue());//nSequencia
        Assertions.assertEquals("44676138000180", row.get("n3q720804").toString().trim());//cnpj
        Assertions.assertTrue(Objects.isNull(row.get("n3q720805")));//cpf
    }
    @Test
    void validarTabelaKwnfe_exportacao() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72323 where n3q720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("n3q720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q720653")).intValue());//sequencia_xml
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q720849")).intValue());//nSequencia
        Assertions.assertEquals("12345678910", row.get("n3q721213").toString().trim());//nDraw
        Assertions.assertEquals(345, ((BigDecimal) row.get("n3q721215")).intValue());//nRE
        Assertions.assertEquals(12345, ((BigDecimal) row.get("n3q721216")).intValue());//chNFe
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q721217")).intValue());//qExport
    }
    @Test
    void validarTabelaKwnfe_pag() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72325 where n3q720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("n3q720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q720849")).intValue());//nSequencia
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q721248")).intValue());//tPag
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q721249")).intValue());//vPag
        Assertions.assertEquals("05054903000179", row.get("n3q721250").toString().trim());//cnpj
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q721251")).intValue());//tBand
        Assertions.assertEquals("1", row.get("n3q721252").toString().trim());//cAut
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q722423")).intValue());//vTroco - Apath não consta na planilha de mapeamento
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q722422")).intValue());//tpIntegra - Apath não consta na planilha de mapeamento
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q720705")).intValue());//indPag - Apath não consta na planilha de mapeamento
    }
    @Test
    void validarTabelaKwnfe_infcomexterior() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72326 where n3q720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("n3q720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q720849")).intValue());//nSequencia
        Assertions.assertEquals("DF", row.get("n3q721253").toString().trim());//ufSaidaPais
        Assertions.assertEquals("DIADEMA", row.get("n3q721254").toString().trim());//xLocExporta
        Assertions.assertEquals("SANTOS", row.get("n3q721255").toString().trim());//xLocDespacho
    }
    @Test
    void validarTabelaKwnfe_cana() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72327 where n3q720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("n3q720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q720849")).intValue());//nSequencia
        Assertions.assertEquals("2022/", row.get("n3q721256").toString().trim());//safra
        Assertions.assertEquals("2023", row.get("n3q721257").toString().trim());//ref
        Assertions.assertEquals(2, ((BigDecimal) row.get("n3q721260")).intValue());//qTotMes
        Assertions.assertEquals(3, ((BigDecimal) row.get("n3q721261")).intValue());//qTotAnt
        Assertions.assertEquals(4, ((BigDecimal) row.get("n3q721262")).intValue());//qTotGer
        Assertions.assertEquals(4, ((BigDecimal) row.get("n3q721265")).intValue());//vFor
        Assertions.assertEquals(5, ((BigDecimal) row.get("n3q721266")).intValue());//vTotDed
        Assertions.assertEquals(6, ((BigDecimal) row.get("n3q721267")).intValue());//vLiqFor
    }
    @Test
    void validarTabelaKwnfe_cana_forndiario() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72328 where n3q720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("n3q720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q720849")).intValue());//nSequencia
        Assertions.assertEquals("1", row.get("n3q721258").toString().trim());//dia
        Assertions.assertEquals(7, ((BigDecimal) row.get("n3q721259")).intValue());//qtde
    }
    @Test
    void validarTabelaKwnfe_cana_deducao() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72329 where n3q720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("n3q720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("n3q720849")).intValue());//nSequencia
        Assertions.assertEquals("xDed", row.get("n3q721263").toString().trim());//xDed
        Assertions.assertEquals(2, ((BigDecimal) row.get("n3q721264")).intValue());//vDed
    }
    @Test
    void validarTabelaKwnfe_rastro() {
        List<Map<String, Object>> result = jdbcTemplate.queryForList
                ("select * from fq72334 where nsq720652 like '%NFe35230856493877000116550010002073651002155823%'");//kwnfe
        Map<String, Object> row = result.get(0);

        Assertions.assertEquals("NFe35230856493877000116550010002073651002155823", row.get("nsq720652").toString().trim());//id
        Assertions.assertEquals(1, ((BigDecimal) row.get("nsq720653")).intValue());//nItem
        Assertions.assertEquals(1, ((BigDecimal) row.get("nsq720849")).intValue());//nSequencia
        Assertions.assertEquals("1", row.get("nsq722400").toString().trim());//nLote
        Assertions.assertEquals(1, ((BigDecimal) row.get("nsq722401")).intValue());//qLote
        Assertions.assertEquals("dFab", row.get("nsq722402").toString().trim());//dFab
        Assertions.assertEquals("dVal", row.get("nsq722403").toString().trim());//dVal
        Assertions.assertEquals(7, ((BigDecimal) row.get("nsq722433")).intValue());//caGreg
    }
    @Test
    void validarTabelaKwnfe_controle() {
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
        Assertions.assertEquals("4.00", row.get("nqq721107").toString().trim());//datechgstatus07 - Apath não consta na planilha de mapeamento
        Assertions.assertEquals(1243, ((BigDecimal) row.get("nqq72165")).intValue());//qtysum
        Assertions.assertEquals("68337658000127", row.get("nqq721103").toString().trim());//cnpj_transport - Apath não consta na planilha de mapeamento
    }
}

