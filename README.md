# Explicação Geral
Nesse repositório contém uma aplicação das API's da Five9 para extrair os relatórios.

***main.py*** - nesse arquivo estão 3 funções que realizam os request's para a API da Five9

***gerar_pdf*** - nesse arquivo contém as funções necessárias para a manipulação dos dados e criação do pdf baseado nos templates

***relatorio.css/relatorio.html*** - template para o pdf

## Importante
É necessário criar um arquivo chamado: credentials.txt que deverá ter o login e senha no seguinte formato, **sem nenhum caracter e nem espaço extra**:
```
username password
```
# Bibliotecas utilizadas

1 - five9

2 - pdfkit

3 - jinja2

4 - datetime

```
pip install five9 pdfkit jinja2 datetime
```
# Execução
Para executar o código, basta alterar as credenciais, a data de início e fim e rodar o código com o seguinte comando:
```
python main.py
```
