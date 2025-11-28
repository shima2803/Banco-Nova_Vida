import pymysql
import pyodbc
import pandas as pd
import paramiko
import sys
from datetime import datetime

CREDENTIALS_FILE = r"\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials.txt"


def carregar_credenciais():
    ns = {"pymysql": pymysql}
    with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
        code = f.read()
    exec(code, ns)
    return ns


creds = carregar_credenciais()
BD_TELEFONES_SQLAUTH = creds.get("BD_TELEFONES_SQLAUTH")
SFTP_DEST = creds.get("SFTP_DEST")

if SFTP_DEST is None:
    print("### ERRO: 'SFTP_DEST' não encontrado em SA_Credencials.txt.")
    sys.exit(1)


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

    return pyodbc.connect(conn_str)


QUERY_PF = "SELECT * FROM Viewpf;"
QUERY_PJ = "SELECT * FROM viewpj;"


def upload_sftp(local_path, remote_dir, remote_filename, host, port, username, password):
    remote_path = f"{remote_dir.rstrip('/')}/{remote_filename}"

    transport = None
    sftp = None
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            sftp.listdir(remote_dir)
        except IOError:
            parts = remote_dir.strip('/').split('/')
            current = ''
            for part in parts:
                current = f"{current}/{part}" if current else f"/{part}"
                try:
                    sftp.listdir(current)
                except IOError:
                    sftp.mkdir(current)

        sftp.put(local_path, remote_path)

    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


def gerar_log(mensagem, sftp_config):
    dt = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_filename = f"log_{dt}.txt"
    local_log_path = f"C:/Windows/Temp/{log_filename}"

    with open(local_log_path, "w", encoding="utf-8") as f:
        f.write(mensagem)

    upload_sftp(
        local_path=local_log_path,
        remote_dir="/preambulo/atualizacao/Logs",
        remote_filename=log_filename,
        host=sftp_config["host"],
        port=sftp_config["port"],
        username=sftp_config["user"],
        password=sftp_config["password"],
    )


def main():
    log_text = ""

    try:
        conn = get_bdtelefones_connection()
        log_text += f"[{datetime.now()}] Conectado ao banco BD_TELEFONES.\n"

        df_pf = pd.read_sql(QUERY_PF, conn)
        df_pj = pd.read_sql(QUERY_PJ, conn)

        log_text += f"[{datetime.now()}] Consultas PF e PJ executadas.\n"

        conn.close()

        OUTPUT_FILE_PF = r"\\fs01\ITAPEVA ATIVAS\DADOS\Base Nova Vida\NV_PF.csv"
        OUTPUT_FILE_PJ = r"\\fs01\ITAPEVA ATIVAS\DADOS\Base Nova Vida\NV_PJ.csv"

        df_pf.to_csv(OUTPUT_FILE_PF, sep=";", index=False, encoding="utf-8-sig")
        df_pj.to_csv(OUTPUT_FILE_PJ, sep=";", index=False, encoding="utf-8-sig")

        log_text += f"[{datetime.now()}] CSV PF e PJ gerados com sucesso.\n"

        upload_sftp(
            local_path=OUTPUT_FILE_PF,
            remote_dir=SFTP_DEST["dir_pf"],
            remote_filename="NV_PF.csv",
            host=SFTP_DEST["host"],
            port=SFTP_DEST["port"],
            username=SFTP_DEST["user"],
            password=SFTP_DEST["password"],
        )

        upload_sftp(
            local_path=OUTPUT_FILE_PJ,
            remote_dir=SFTP_DEST["dir_pj"],
            remote_filename="NV_PJ.csv",
            host=SFTP_DEST["host"],
            port=SFTP_DEST["port"],
            username=SFTP_DEST["user"],
            password=SFTP_DEST["password"],
        )

        log_text += f"[{datetime.now()}] Upload de PF e PJ realizado com sucesso.\n"

    except Exception as e:
        log_text += f"[{datetime.now()}] ERRO: {str(e)}\n"

    finally:
        gerar_log(log_text, SFTP_DEST)


if __name__ == "__main__":
    main()
