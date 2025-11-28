# 📊 Banco Nova Vida – Exportador de Bases (Python)

Automação em Python para consultar o banco **BD_TELEFONES** (SQL Server) e gerar duas bases em **CSV**:

- `NV_PF.csv` → Pessoas Físicas  
- `NV_PJ.csv` → Pessoas Jurídicas  

Os arquivos são salvos em uma pasta de rede para uso em outras rotinas/sistemas.

---

## 🧩 Visão Geral

O script:

1. Lê credenciais a partir de um arquivo de texto em rede:  
   `\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials.txt`
2. Abre conexão com o SQL Server usando **SQL Auth**.
3. Executa as views:
   - `Viewpf` (PF)
   - `viewpj` (PJ)
4. Exporta o resultado para dois arquivos CSV:

```text
\\fs01\ITAPEVA ATIVAS\DADOS\Base Nova Vida\NV_PF.csv
\\fs01\ITAPEVA ATIVAS\DADOS\Base Nova Vida\NV_PJ.csv
Os arquivos são sempre sobrescritos, usando:

separador: ;

encoding: utf-8-sig

\\
```
## 🔐 Segurança

✅ As senhas e dados sensíveis NÃO ficam no código.
Todas as credenciais foram retiradas do script Python e movidas para o arquivo:
```text
\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials.txt
```
Vantagens dessa abordagem:

O código pode ser versionado no GitHub sem expor usuário/senha.

A troca de senha é feita alterando apenas o TXT, sem editar o script.

As credenciais ficam centralizadas em um único arquivo na rede.

⚠ Atenção: o arquivo SA_Credencials.txt é carregado via exec().
Por isso, ele deve ter permissões de edição restritas (somente equipe autorizada).

## ▶️ Como Executar

No diretório do script:
```text
python script.py
```

Pré-requisitos:

Acesso à pasta de rede onde está o SA_Credencials.txt

Python 3.x instalado

## 📦 Dependências

Instale as bibliotecas necessárias com:
```text
pip install pandas pyodbc pymysql
```

pandas → leitura das queries e geração dos CSVs

pyodbc → conexão com o SQL Server

pymysql → necessário para a forma como o arquivo de credenciais é carregado


## 💾 Resumo:
Este projeto automatiza a extração de dados PF/PJ do banco BD_TELEFONES, gera dois CSVs padronizados e mantém as credenciais fora do código, aumentando a segurança e permitindo versionamento seguro no GitHub.


Se quiser, no próximo passo posso te ajudar a adicionar isso ao repositório (comandos `git add`, `commit`, `push`).
