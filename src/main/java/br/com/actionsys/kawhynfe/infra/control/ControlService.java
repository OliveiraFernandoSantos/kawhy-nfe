package br.com.actionsys.kawhynfe.infra.control;

import br.com.actionsys.kawhycommons.Constants;
import br.com.actionsys.kawhycommons.infra.util.DateUtil;
import br.com.actionsys.kawhycommons.integration.IntegrationContext;
import br.com.actionsys.kawhycommons.types.KawhyType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static br.com.actionsys.kawhycommons.infra.util.APathUtil.execute;
import static br.com.actionsys.kawhycommons.infra.util.APathUtil.executeOrNull;

@Service
public class ControlService {

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Value("${dir.saida}")
    private String outputFolder;

    public void insert(IntegrationContext item) {
        Document d = item.getDocument();
        Map<String, Object> v = new HashMap<>();

        v.put("nqq720652", item.getId());//nfe_id
        v.put("nqq720841", executeOrNull(d, "nfeProc/protNFe/infProt/nProt"));//protocolo
        v.put("nqq720708", executeOrNull(d, "nfeProc/NFe/infNFe/ide/nNF"));//nnf
        v.put("nqq720707", executeOrNull(d, "nfeProc/NFe/infNFe/ide/serie"));//serie
        v.put("nqq720804", executeOrNull(d, "nfeProc/NFe/infNFe/emit/CNPJ"));//cnpj_emit
        v.put("nqq720814", executeOrNull(d, "nfeProc/NFe/infNFe/emit/enderEmit/UF"));//uf_emit
        v.put("nqq720812", executeOrNull(d, "nfeProc/NFe/infNFe/emit/enderEmit/xMun"));//xmun_emit
        v.put("nqq720842", executeOrNull(d, "nfeProc/NFe/infNFe/dest/CNPJ"));//cnpj_dest
        v.put("nqq720854", executeOrNull(d, "nfeProc/NFe/infNFe/dest/CPF"));//cpf_dest
        v.put("nqq720856", executeOrNull(d, "nfeProc/NFe/infNFe/dest/enderDest/UF"));//uf_dest
        v.put("nqq720813", executeOrNull(d, "nfeProc/NFe/infNFe/dest/enderDest/xMun"));//xmun_dest
        v.put("nqq720709", execute(d, "nfeProc/NFe/infNFe/ide/dhEmi").substring(0, 10));//demi
        v.put("nqq720961", executeOrNull(d, "nfeProc/protNFe/infProt/cStat"));//cstatussefaz
        v.put("nqq720833", executeOrNull(d, "nfeProc/protNFe/infProt/xMotivo"));//xmotivo

        Date dateNow = new Date();
        v.put("nqq720962", DateUtil.formatDateToDb(dateNow));//dtconsultasefaz
        v.put("nqq720963", DateUtil.formatTimeToDb(dateNow));//hrconsultasefaz

        v.put("nqq720973", outputFolder.concat(item.getFile().getName()));//arquivo

        v.put("nqq720972", DateUtil.formatDateToDb(dateNow));//dtrecepcao

        v.put("nqq720863", Constants.KAWHY);//audit_usuario
        v.put("nqq720870", KawhyType.KAWHY_NFE.getAuditName());//audit_programa

        v.put("nqq720881", DateUtil.formatDateToDb(dateNow));//audit_data
        v.put("nqq720874", DateUtil.formatTimeToDb(dateNow));//audit_hora

        v.put("nqq721103", executeOrNull(d,"nfeProc/NFe/infNFe/transp/transporta/CNPJ"));//cnpj_transport

        v.put("nqq721107", executeOrNull(d, "nfeProc/NFe/infNFe/@versao"));//datechgstatus07
//        v.put("nqq72164", executeOrNull(d,"nfeProc/NFe/infNFe/det/prod/qCom"));//qtysum
        v.put("nqq72165", new BigDecimal(execute(d, "nfeProc/NFe/infNFe/total/ICMSTot/vNF")));//vlrnf

        v.put("nqq720980", Constants.SPACE);//cstatus01
        v.put("nqq720983", Constants.SPACE);//cstatus02
        v.put("nqq720986", Constants.SPACE);//cstatus03
        v.put("nqq720989", Constants.SPACE);//cstatus04
        v.put("nqq720992", Constants.SPACE);//cstatus05
        v.put("nqq721101", Constants.SPACE);//cstatus06
        v.put("nqq721105", Constants.SPACE);//cstatus07
        v.put("nqq721108", Constants.SPACE);//cstatus08
        v.put("nqq721111", Constants.SPACE);//cstatus09
        v.put("nqq721114", Constants.SPACE);//cstatus10
        v.put("nqq721117", Constants.SPACE);//cstatus11
        v.put("nqq721120", Constants.SPACE);//cstatus12
        v.put("nqq721123", Constants.SPACE);//cstatus13
        v.put("nqq721126", Constants.SPACE);//cstatus14
        v.put("nqq721129", Constants.SPACE);//cstatus15
        v.put("nqq721132", Constants.SPACE);//cstatus16
        v.put("nqq721135", Constants.SPACE);//cstatus17
        v.put("nqq72138", Constants.SPACE);//cstatus18
        v.put("nqq72141", Constants.SPACE);//cstatus19
        v.put("nqq72144", Constants.SPACE);//cstatus20

        SimpleJdbcInsert simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate);
        simpleJdbcInsert.setTableName("fq72317");//KWNFE_CONTROLE
        simpleJdbcInsert.execute(v);
    }

    public void updateToCancel(IntegrationContext item) {
        Document d = item.getDocument();

        // statusnfe, cstatussefaz, xmotivo, auditdata, audithora, canc_prot, canc_id, canc_motivo, canc_data, canc_hora
        String sql = "update fq72317 set nqq72162 = ?, nqq720961 = ?, nqq720833 = ?, nqq720881 = ?, nqq720874 = ?, nqq721173 = ?, nqq721174 = ?, nqq721175 = ?, nqq721176 = ?, nqq721177 = ?  where nqq720652 like ? ";

        Date dateNow = new Date();
        String xMotivo = execute(d, "procEventoNFe/retEvento/infEvento/xMotivo");
        String auditData = DateUtil.formatDateToDb(dateNow);
        String auditHora = DateUtil.formatTimeToDb(dateNow);
        String nProt = execute(d, "procEventoNFe/evento/infEvento/detEvento/nProt");
        String xJust = execute(d, "procEventoNFe/evento/infEvento/detEvento/xJust");
        String dataCancel = DateUtil.formatDateSefazToDb(execute(d, "procEventoNFe/evento/infEvento/dhEvento"));
        String timeCancel = DateUtil.formatTimeSefazToDb(execute(d, "procEventoNFe/evento/infEvento/dhEvento"));

        jdbcTemplate.update(sql,
                Constants.DESCRIPTION_CANCEL_DOCUMENT, Constants.CSTATUS_SEFAZ_CACELAMENTO, xMotivo, auditData, auditHora, nProt, item.getId(), xJust, dataCancel, timeCancel, item.getId() + "%");
    }

    public boolean hasNfeControle(String id) {
        List<Map<String, Object>> result = jdbcTemplate.queryForList("select 1 from fq72317 where nqq720652 Like ?", id + "%");

        return !result.isEmpty();
    }

}
