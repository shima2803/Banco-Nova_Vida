import pymysql
import pyodbc
import pandas as pd

CREDENTIALS_FILE = r"\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials.txt"

def carregar_credenciais():
    ns = {"pymysql": pymysql}
    with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
        code = f.read()
    exec(code, ns)
    return ns

creds = carregar_credenciais()
BD_TELEFONES_SQLAUTH = creds.get("BD_TELEFONES_SQLAUTH")

def get_bdtelefones_connection():
    cfg = BD_TELEFONES_SQLAUTH

    conn_str = (
        f"DRIVER={{{cfg['driver']}}};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['user']};"
        f"PWD={cfg['password']};"
        "Encrypt=no;"
        f"TrustServerCertificate={cfg['trust_server_certificate']};"
    )

    conn = pyodbc.connect(conn_str)
    print("Conexão BD_TELEFONES via SQL Auth (sa) OK.")
    return conn

QUERY_PF = """
SELECT * 
FROM Viewpf;
"""

QUERY_PJ = """
SELECT * 
FROM viewpj;
"""

def main():
    conn = get_bdtelefones_connection()

    print("Executando consulta PF...")
    df_pf = pd.read_sql(QUERY_PF, conn)

    print("Executando consulta PJ...")
    df_pj = pd.read_sql(QUERY_PJ, conn)

    conn.close()

    OUTPUT_FILE_PF = r"\\fs01\ITAPEVA ATIVAS\DADOS\Base Nova Vida\NV_PF.csv"
    OUTPUT_FILE_PJ = r"\\fs01\ITAPEVA ATIVAS\DADOS\Base Nova Vida\NV_PJ.csv"

    print(f"Salvando PF em: {OUTPUT_FILE_PF}")
    df_pf.to_csv(OUTPUT_FILE_PF, sep=";", index=False, encoding="utf-8-sig")

    print(f"Salvando PJ em: {OUTPUT_FILE_PJ}")
    df_pj.to_csv(OUTPUT_FILE_PJ, sep=";", index=False, encoding="utf-8-sig")

    print("\n✔ Arquivos CSV criados/atualizados com sucesso!")
    print(f"PF: {OUTPUT_FILE_PF}")
    print(f"PJ: {OUTPUT_FILE_PJ}")

if __name__ == "__main__":
    main()
