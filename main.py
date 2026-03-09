# =========================================================
# 1. IMPORTS E TOKEN DO LINKED SERVICE
# =========================================================
import pandas as pd
from notebookutils import mssparkutils

# Carrega o token do Linked Service para o SQL Dedicado
token = TokenLibrary.getConnectionString("AzureSqlDedicatedDatabase")

# =========================================================
# 2. CONFIGURAÇÃO DO AMBIENTE & JDBC
# =========================================================
ambiente = "prp"   # dev / prp / prd

jdbcHostname = f"synw-azbr-0001-{ambiente}-mgmt.sql.azuresynapse.net"
jdbcDatabase = "dpazbr000101"
jdbcPort = 1433

jdbcUrl = (
    f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};"
    f"database={jdbcDatabase};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"hostNameInCertificate=*.sql.azuresynapse.net;"
)

connectionProperties = {
    "accessToken": token,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print("🔗 Conexão JDBC configurada com sucesso.")

env = 'prp'

path_excel = "abfss://sac-core-f0001@stazbr0001prpext.dfs.core.windows.net/uploads_do/equalizacao_ambientes/equalizar_prp.xlsx"

df = pd.read_excel(path_excel)
df.head()

#---------------------------------------
# =========================================
# 1) Imports e utilitários
# =========================================
import re
import pandas as pd

# Acesso ao JVM do Spark (já disponível no Synapse)
jvm = spark._sc._jvm

# Util: dividir script por GO (linhas com GO isolado, case-insensitive)
go_splitter = re.compile(r'^\s*GO\s*$', re.IGNORECASE | re.MULTILINE)

def split_batches(sql_text: str):
    if sql_text is None:
        return []
    parts = [p.strip() for p in go_splitter.split(sql_text)]
    return [p for p in parts if p]  # remove vazios


# =========================================
# 2) Conexão JDBC sem pacotes externos
#    - usa DriverManager do Java
#    - usa o accessToken do Linked Service
# =========================================

# Access token do Linked Service (igual você já usa)
token = TokenLibrary.getConnectionString("AzureSqlDedicatedDatabase")

ambiente = "prp"  # dev | prp | prd
jdbcHostname = f"synw-azbr-0001-{ambiente}-mgmt.sql.azuresynapse.net"
jdbcDatabase = "dpazbr000101"
jdbcPort = 1433

jdbcUrl = (
    f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};"
    f"database={jdbcDatabase};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"hostNameInCertificate=*.sql.azuresynapse.net;"
)

# Cria um java.util.Properties e injeta o accessToken
props = jvm.java.util.Properties()
props.setProperty("accessToken", token)

# DriverManager e tipos Java
DriverManager = jvm.java.sql.DriverManager


def exec_tsql(sql_text: str):
    """
    Executa T-SQL (DDL/DML) no Dedicated via JDBC puro (DriverManager).
    Divide por GO, executa batch a batch com Statement.execute().
    Retorna (True, None) se OK; (False, 'msg') se erro.
    """
    conn = None
    stmt = None
    try:
        # Abre a conexão
        conn = DriverManager.getConnection(jdbcUrl, props)
        conn.setAutoCommit(True)
        stmt = conn.createStatement()

        # Divide por GO e executa
        batches = split_batches(sql_text)
        for b in batches:
            if b.strip():
                stmt.execute(b)

        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            if stmt is not None:
                stmt.close()
        except:
            pass
        try:
            if conn is not None:
                conn.close()
        except:
            pass


# =========================================
# 4) Loop de execução + logs + prints
# =========================================
logs = []

for idx, row in df.iterrows():
    tabela = row.get("Tabela")
    script = row.get("CreateTableScript")

    print("\n=====================================================")
    print(f"▶ PROCESSANDO: {tabela}")
    print("▶ SCRIPT:")
    print(script)
    print("=====================================================")

    if script is None or str(script).strip() == "":
        print(f"❌ ERRO: Script vazio → {tabela}")
        logs.append({"Tabela": tabela, "Status": "ERROR", "Mensagem": "Script vazio"})
        continue

    ok, msg = exec_tsql(script)

    if ok:
        print(f"✔ SUCESSO: {tabela}")
        logs.append({"Tabela": tabela, "Status": "SUCCESS", "Mensagem": ""})
    else:
        print(f"❌ ERRO: {tabela}")
        print(msg)
        logs.append({"Tabela": tabela, "Status": "ERROR", "Mensagem": msg})


# =========================================
# 5) Log final
# =========================================
log_df = pd.DataFrame(logs)
display(log_df)
