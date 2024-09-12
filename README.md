# KawhyNfe
Importação de notas fiscais para as tabelas do GRA.

## Requisitos
Para executar o projeto, será necessário instalar os seguintes programas:
* JDK 8: Necessário para executar o projeto Java

#$ Mapeamento tabelas x tags do GRA
https://docs.google.com/spreadsheets/d/1AvmeZvnBTyf-4yhgQxDSWaY7Cg0R7y6jCDafygZna5U/edit#gid=456457777

## Executanto projeto para desenvolvimento
--spring.profiles.active=local

## Container docker com oracle para rodar local
**Criar container**
> docker run --name oracle-xe-gra -d -p 1521:1521 --env
> ORACLE_PASSWORD=password --env APP_USER=GRA_USER --env
> APP_USER_PASSWORD=password gvenzl/oracle-xe

**Parar o container**
> docker stop oracle-xe-gra

**Executar container**
> docker start oracle-xe-gra

# Histórico de Versões

### 6.0.0
Refatoramos o KawhyNfe, buscando preservar e não mexer no código que faz a leitura do xml e escreve no banco dados, dessa forma o mapeamente da tag xml para a coluna do banco de dados não fosse alterado.

Melhorias 
* Implementação de controle de exceção.
* Redefinir arquivo de log, adiciando infirmações como nfeid, nome do arquivo processado.
* Refataramos a lógica do fluxo que o arquivo percorre dentro da apliciação em caso de sucesso e falha.
* Refatoração do controle de transação, garantindo que não ocorra modificações no banco de dados casso ocorra um erro. (Não está salvando os dados da tabela de controle)
* Remover classes que não estavam em usso.
* Remover configurações do arquivo de properties que não exite mais implementação, ou seja não surgia efeito na aplicação, com url de serviços da sefaz.
* Implementar SpringBoot e sua gestão de injeções de dependências.
* Será necessário adicionar um parâmetro no serviços do Kawhy, hoje no arquivo install.
* Removendo referência para buscar o valor do GRA_HOME no geristro do windows. Permitindo executar em servidores linux e containers.
* Suporte a infra extrutura de test.