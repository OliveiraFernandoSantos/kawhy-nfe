package br.com.actionsys.kawhynfe;

import br.com.actionsys.kawhycommons.Constants;
import br.com.actionsys.kawhycommons.infra.exception.DuplicateFileException;
import br.com.actionsys.kawhycommons.infra.exception.IgnoreFileException;
import br.com.actionsys.kawhycommons.infra.exception.OtherFileException;
import br.com.actionsys.kawhycommons.infra.util.APathUtil;
import br.com.actionsys.kawhycommons.infra.util.DocumentUtil;
import br.com.actionsys.kawhycommons.integration.IntegrationContext;
import br.com.actionsys.kawhycommons.integration.IntegrationOrchestrator;
import br.com.actionsys.kawhycommons.types.DocumentType;
import br.com.actionsys.kawhycommons.types.KawhyType;
import br.com.actionsys.kawhyimport.metadata.ImportService;
import br.com.actionsys.kawhynfe.infra.control.ControlService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Slf4j
@Component
public class Orchestrator extends IntegrationOrchestrator {

    @Autowired
    private ImportService importService;
    @Autowired
    private ControlService controlService;

    @Override
    public void processDocumentFile(IntegrationContext item) throws Exception {
        item.setId(APathUtil.execute(item.getDocument(), "nfeProc/NFe/infNFe/@Id"));

        if (controlService.hasNfeControle(item.getId())) {
            throw new DuplicateFileException();
        }

        String cnpjDest = APathUtil.execute(item.getDocument(), "nfeProc/NFe/infNFe/dest/CNPJ");
        String cnpjEmit = APathUtil.execute(item.getDocument(), "nfeProc/NFe/infNFe/emit/CNPJ");

        if (!licenseService.validateCnpjs(Arrays.asList(cnpjDest, cnpjEmit))) {
            throw new OtherFileException("CNPJ sem licença Dest : " + cnpjDest + " Emit : " + cnpjEmit + " - " + item.getFile().getAbsolutePath());
        }

        if (isDevolucao(item) && !licenseService.hasDevolucao(cnpjDest) && !licenseService.hasDevolucao(cnpjEmit)) {
            throw new OtherFileException("A licença não ativada para Nfe de devolução, Dest : " + cnpjDest + " Emit : " + cnpjEmit + " - " + item.getFile().getAbsolutePath());
        }

        //Processa tabelas do documento fiscal.
        importService.process(item);

        //Salvar informções da NFe na Tabela fq72317/kwnfe_controle
        controlService.insert(item);
    }

    @Override
    public void processCancelFile(IntegrationContext item) throws Exception {
        item.setId(DocumentType.NFE.getPrefix() + APathUtil.execute(item.getDocument(), "procEventoNFe/evento/infEvento/chNFe"));

        if (!controlService.hasNfeControle(item.getId())) {
            throw new IgnoreFileException("Evento de cancelamento aguardando documento fiscal : " + item.getFile().getAbsolutePath());
        }

        controlService.updateToCancel(item);
    }

    @Override
    public boolean isCancel(IntegrationContext item) throws Exception {
        return Constants.NFE_SEFAZ_EVENTO_TP_CANCELAMENTO.equals(APathUtil.execute(item.getDocument(), "procEventoNFe/evento/infEvento/tpEvento"));
    }

    public boolean isDevolucao(IntegrationContext item) throws Exception {
        return Constants.NFE_DEVOLUCAO.equals(APathUtil.execute(item.getDocument(), "nfeProc/NFe/infNFe/ide/finNFe"));
    }

    @Override
    public boolean isDocument(IntegrationContext item) throws Exception {
        return DocumentUtil.isNfe(item.getDocument());
    }

    @Override
    public KawhyType getKawhyType() {
        return KawhyType.KAWHY_NFE;
    }

}
