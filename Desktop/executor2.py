import json
import pymysql
import pyodbc
from datetime import datetime, timedelta
from nova_vida_client import NovaVidaClient

CREDENTIALS_FILE = r"\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials.txt"


def carregar_credenciais():
    ns = {"pymysql": pymysql}
    try:
        with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
            code = f.read()
        exec(code, ns)
    except Exception as e:
        print("\n### ERRO AO CARREGAR CREDENCIAIS ###")
        print(f"Arquivo: {CREDENTIALS_FILE}")
        print(e)
        raise
    return ns


creds = carregar_credenciais()

client = NovaVidaClient(
    usuario=creds.get("usuario"),
    senha=creds.get("senha"),
    cliente=creds.get("cliente"),
)

MYSQL_CONFIG = creds.get("MYSQL_CONFIG")


def get_mysql_connection():
    conn = pymysql.connect(**MYSQL_CONFIG)
    print("Conexão MySQL (GECOBI) OK.")
    return conn


BD_TELEFONES_WINDOWS = creds.get("BD_TELEFONES_WINDOWS")
BD_TELEFONES_SQLAUTH = creds.get("BD_TELEFONES_SQLAUTH")


def get_bdtelefones_connection():
    try:
        cfg = BD_TELEFONES_WINDOWS
        conn_str = (
            f"DRIVER={{{cfg['driver']}}};"
            f"SERVER={cfg['server']};"
            f"DATABASE={cfg['database']};"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str)
        print("Conexão BD_TELEFONES via Windows Auth OK.")
        return conn
    except Exception as e:
        print("Windows Auth falhou, tentando SQL Auth...", e)

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
    print("Conexão BD_TELEFONES via SQL Auth OK.")
    return conn


def to_int(v):
    if v in (None, "", " "):
        return None
    try:
        return int(v)
    except Exception:
        return None


def to_float(v):
    if v in (None, "", " "):
        return None
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None


def parse_date_br(s):
    if not s or s in ("", "00/00/0000"):
        return None
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except Exception:
        return None


def get_cadast(resp):
    return resp.get("d", {}).get("CONSULTA", {}).get("CADASTRAIS", {}) or {}


def get_blocos(resp):
    return resp.get("d", {}).get("CONSULTA", {}) or {}


def deve_consultar_documento(documento, cache_ultima_consulta):
    info = cache_ultima_consulta.get(documento)
    if info is None:
        return True

    dt = info.get("data")
    if dt is None:
        return True

    if not isinstance(dt, datetime):
        try:
            dt = datetime(dt.year, dt.month, dt.day)
        except Exception:
            return True

    limite = datetime.now() - timedelta(days=90)
    return dt < limite


def upsert_cadastro_unificado(cur_sql, resp, documento):
    cad = get_cadast(resp)
    if not cad:
        return

    if "CNPJ" in cad:
        cnpj = cad.get("CNPJ") or documento
        if not cnpj:
            return

        razao = cad.get("RAZAO") or cad.get("NOME_FANTASIA") or cnpj
        nome_fantasia = cad.get("NOME_FANTASIA") or None

        matriz = cad.get("MATRIZ")
        qtde_filiais = to_int(cad.get("QTDE_FILIAIS"))
        ab18 = to_int(cad.get("TOTAL_FILIAIS_ABERTAS_18M"))
        ab12 = to_int(cad.get("TOTAL_FILIAIS_ABERTAS_12M"))
        ab6 = to_int(cad.get("TOTAL_FILIAIS_ABERTAS_06M"))
        data_abertura = parse_date_br(cad.get("DATA_ABERTURA"))
        cod_natjur = cad.get("CODNATJUR")
        desc_natjur = cad.get("DESC_NATJUR")
        cnae = cad.get("CNAE")
        desc_cnae = cad.get("DESC_CNAE")
        atividade_sec = cad.get("ATIVIDADE-ECONOMICA-SECUNDARIA")
        insc_est = cad.get("INSCRICAO_ESTADUAL")
        porte = cad.get("PORTE")
        fatur_pres = to_float(cad.get("FATURAMENTOPRESUMIDO"))
        capital_social = to_float(cad.get("CAPITALSOCIAL"))
        qtde_func = to_int(cad.get("QTDEFUNCIONARIOS"))
        flag_dau = cad.get("FLAG_DAU")

        score = to_int(cad.get("SCORE"))
        msg_score = cad.get("MENSAGEMSCORE")

        cur_sql.execute(
            """
            UPDATE dbo.CadastrosUnificados
               SET NomeUnificado = ?,
                   NomeFantasia = ?,
                   RG = NULL,
                   OrgaoEmissor = NULL,
                   Sexo = NULL,
                   Nascimento = NULL,
                   Idade = NULL,
                   Geracao = NULL,
                   Signo = NULL,
                   PossivelProfissao = NULL,
                   PossivelEscolaridade = NULL,
                   PersonaDemografica = NULL,
                   Renda = NULL,
                   ClasseEconomica = NULL,
                   EstadoCivil = NULL,
                   Nacionalidade = NULL,
                   AuxilioBrasil = NULL,
                   FlagAglomeradoSubnormal = NULL,
                   FonteDeRenda = NULL,
                   TempoEmpreendedor = NULL,
                   CodigoCbo = NULL,
                   Matriz = ?,
                   QtdeFiliais = ?,
                   Abertas18M = ?,
                   Abertas12M = ?,
                   Abertas06M = ?,
                   DataAbertura = ?,
                   CodNatJur = ?,
                   DescNatJur = ?,
                   Cnae = ?,
                   DescCnae = ?,
                   AtividadeSecundaria = ?,
                   InscricaoEstadual = ?,
                   Porte = ?,
                   FaturamentoPresumido = ?,
                   CapitalSocial = ?,
                   QtdeFuncionarios = ?,
                   FlagDau = ?,
                   Score = ?,
                   MensagemScore = ?
             WHERE CpfCnpj = ?
            """,
            (
                razao,
                nome_fantasia,
                matriz,
                qtde_filiais,
                ab18,
                ab12,
                ab6,
                data_abertura,
                cod_natjur,
                desc_natjur,
                cnae,
                desc_cnae,
                atividade_sec,
                insc_est,
                porte,
                fatur_pres,
                capital_social,
                qtde_func,
                flag_dau,
                score,
                msg_score,
                cnpj,
            )
        )

        if cur_sql.rowcount == 0:
            cur_sql.execute(
                """
                INSERT INTO dbo.CadastrosUnificados (
                    CpfCnpj,
                    NomeUnificado,
                    NomeFantasia,
                    RG, OrgaoEmissor, Sexo, Nascimento, Idade,
                    Geracao, Signo, PossivelProfissao, PossivelEscolaridade,
                    PersonaDemografica, Renda, ClasseEconomica, EstadoCivil,
                    Nacionalidade, AuxilioBrasil, FlagAglomeradoSubnormal,
                    FonteDeRenda, TempoEmpreendedor, CodigoCbo,
                    Matriz, QtdeFiliais, Abertas18M, Abertas12M, Abertas06M,
                    DataAbertura, CodNatJur, DescNatJur, Cnae, DescCnae,
                    AtividadeSecundaria, InscricaoEstadual, Porte,
                    FaturamentoPresumido, CapitalSocial, QtdeFuncionarios,
                    FlagDau,
                    Score, MensagemScore
                )
                VALUES (
                    ?, ?, ?,
                    NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?,
                    ?, ?
                )
                """,
                (
                    cnpj,
                    razao,
                    nome_fantasia,
                    matriz,
                    qtde_filiais,
                    ab18,
                    ab12,
                    ab6,
                    data_abertura,
                    cod_natjur,
                    desc_natjur,
                    cnae,
                    desc_cnae,
                    atividade_sec,
                    insc_est,
                    porte,
                    fatur_pres,
                    capital_social,
                    qtde_func,
                    flag_dau,
                    score,
                    msg_score,
                )
            )

    else:
        cpf = cad.get("CPF") or documento
        if not cpf:
            return

        nome = cad.get("NOME") or cpf
        rg = cad.get("RG")
        if rg and rg.strip().lower() == "nada consta":
            rg = None

        orgao_emissor = cad.get("ORGAOEMISSOR")
        sexo = cad.get("SEXO")
        nasc = parse_date_br(cad.get("NASC"))
        idade = to_int(cad.get("IDADE"))
        geracao = cad.get("GERACAO")
        signo = cad.get("SIGNO")
        possivel_prof = cad.get("POSSIVELPROFISSAO")
        possivel_esc = cad.get("POSSIVELESCOLARIDADE")
        persona_demo = cad.get("PERSONADEMOGRAFICA")
        renda = to_float(cad.get("RENDA"))
        classe_econ = cad.get("CLASSEECONOMICA")
        estado_civil = cad.get("ESTADOCIVIL")
        nacionalidade = cad.get("NACIONALIDADE")
        aux_brasil = cad.get("AUXILIOBRASIL")
        flag_aglo = cad.get("FLAG_AGLOMERADO_SUBNORMAL")
        fonte_renda = cad.get("FONTE_DE_RENDA")
        tempo_empreend = cad.get("TEMPO_EMPREENDEDOR")
        codigo_cbo = cad.get("CODIGO_CBO")
        flag_dau = cad.get("DIVIDAATIVADAUNIAO_FLAG_DAU")

        score = to_int(cad.get("SCORE"))
        msg_score = cad.get("MENSAGEMSCORE")

        cur_sql.execute(
            """
            UPDATE dbo.CadastrosUnificados
               SET NomeUnificado = ?,
                   NomeFantasia = NULL,
                   RG = ?,
                   OrgaoEmissor = ?,
                   Sexo = ?,
                   Nascimento = ?,
                   Idade = ?,
                   Geracao = ?,
                   Signo = ?,
                   PossivelProfissao = ?,
                   PossivelEscolaridade = ?,
                   PersonaDemografica = ?,
                   Renda = ?,
                   ClasseEconomica = ?,
                   EstadoCivil = ?,
                   Nacionalidade = ?,
                   AuxilioBrasil = ?,
                   FlagAglomeradoSubnormal = ?,
                   FonteDeRenda = ?,
                   TempoEmpreendedor = ?,
                   CodigoCbo = ?,
                   Matriz = NULL,
                   QtdeFiliais = NULL,
                   Abertas18M = NULL,
                   Abertas12M = NULL,
                   Abertas06M = NULL,
                   DataAbertura = NULL,
                   CodNatJur = NULL,
                   DescNatJur = NULL,
                   Cnae = NULL,
                   DescCnae = NULL,
                   AtividadeSecundaria = NULL,
                   InscricaoEstadual = NULL,
                   Porte = NULL,
                   FaturamentoPresumido = NULL,
                   CapitalSocial = NULL,
                   QtdeFuncionarios = NULL,
                   FlagDau = ?,
                   Score = ?,
                   MensagemScore = ?
             WHERE CpfCnpj = ?
            """,
            (
                nome,
                rg,
                orgao_emissor,
                sexo,
                nasc,
                idade,
                geracao,
                signo,
                possivel_prof,
                possivel_esc,
                persona_demo,
                renda,
                classe_econ,
                estado_civil,
                nacionalidade,
                aux_brasil,
                flag_aglo,
                fonte_renda,
                tempo_empreend,
                codigo_cbo,
                flag_dau,
                score,
                msg_score,
                cpf,
            )
        )

        if cur_sql.rowcount == 0:
            cur_sql.execute(
                """
                INSERT INTO dbo.CadastrosUnificados (
                    CpfCnpj,
                    NomeUnificado,
                    NomeFantasia,
                    RG, OrgaoEmissor, Sexo, Nascimento, Idade,
                    Geracao, Signo, PossivelProfissao, PossivelEscolaridade,
                    PersonaDemografica, Renda, ClasseEconomica, EstadoCivil,
                    Nacionalidade, AuxilioBrasil, FlagAglomeradoSubnormal,
                    FonteDeRenda, TempoEmpreendedor, CodigoCbo,
                    Matriz, QtdeFiliais, Abertas18M, Abertas12M, Abertas06M,
                    DataAbertura, CodNatJur, DescNatJur, Cnae, DescCnae,
                    AtividadeSecundaria, InscricaoEstadual, Porte,
                    FaturamentoPresumido, CapitalSocial, QtdeFuncionarios,
                    FlagDau,
                    Score, MensagemScore
                )
                VALUES (
                    ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    ?,
                    ?, ?
                )
                """,
                (
                    cpf,
                    nome,
                    None,
                    rg,
                    orgao_emissor,
                    sexo,
                    nasc,
                    idade,
                    geracao,
                    signo,
                    possivel_prof,
                    possivel_esc,
                    persona_demo,
                    renda,
                    classe_econ,
                    estado_civil,
                    nacionalidade,
                    aux_brasil,
                    flag_aglo,
                    fonte_renda,
                    tempo_empreend,
                    codigo_cbo,
                    flag_dau,
                    score,
                    msg_score,
                )
            )


def inserir_nv_geral(cur, resp, documento):
    cad = get_cadast(resp)

    try:
        tipo_pessoa = client.extrair_tipo_pessoa(resp)
    except Exception:
        tipo_pessoa = "PJ" if "CNPJ" in cad else "PF"

    doc = cad.get("CNPJ") or cad.get("CPF") or documento
    score_nv = to_int(cad.get("SCORE"))
    msg_score = cad.get("MENSAGEMSCORE")

    json_bruto = json.dumps(resp, ensure_ascii=False)

    cur.execute(
        """
        INSERT INTO dbo.NV_Geral
            (Documento, TipoPessoa, IdPessoa, ScoreNV, MensagemScoreNV, JsonBruto)
        OUTPUT INSERTED.IdConsulta
        VALUES (?, ?, NULL, ?, ?, ?)
        """,
        (doc, tipo_pessoa, score_nv, msg_score, json_bruto),
    )
    id_consulta = cur.fetchone()[0]
    return id_consulta, tipo_pessoa


def inserir_nv_enderecos(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    enderecos = blocos.get("ENDERECOS", []) or []

    for end in enderecos:
        if not isinstance(end, dict):
            continue

        pos = to_int(end.get("POSICAO"))
        if pos is None:
            pos = 0

        tipo = end.get("TIPO")
        logradouro = end.get("LOGRADOURO")
        numero = end.get("NUMERO")
        compl = end.get("COMPLEMENTO")
        bairro = end.get("BAIRRO")
        cidade = end.get("CIDADE")
        uf = end.get("UF")
        cep = end.get("CEP")
        arearisco = end.get("AREARISCO")
        lat = to_float(end.get("LATITUDE"))
        lon = to_float(end.get("LONGITUDE"))

        cur.execute(
            """
            INSERT INTO dbo.NV_Endereco
                (IdConsulta, Posicao, Tipo, Logradouro, Numero, Complemento,
                 Bairro, Cidade, UF, CEP, AreaRisco, Latitude, Longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                id_consulta,
                pos,
                tipo,
                logradouro,
                numero,
                compl,
                bairro,
                cidade,
                uf,
                cep,
                arearisco,
                lat,
                lon,
            ),
        )


def inserir_nv_telefones(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    tels = blocos.get("TELEFONES", []) or []

    for t in tels:
        if not isinstance(t, dict):
            continue

        pos = to_int(t.get("POSICAO"))
        if pos is None:
            pos = 0

        ddd = t.get("DDD") or "00"
        numero = t.get("TELEFONE") or ""
        assinante = t.get("ASSINANTE")
        tipo_tel = t.get("TIPO_TELEFONE")
        procon = t.get("PROCON")
        operadora = t.get("OPERADORA")
        flhot = t.get("FLHOT")
        flwhats = t.get("FLWHATS")

        cur.execute(
            """
            INSERT INTO dbo.NV_Telefone
                (IdConsulta, Posicao, Ddd, Numero, Assinante,
                 TipoTelefone, Procon, Operadora, FlHot, FlWhats)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                id_consulta,
                pos,
                ddd,
                numero,
                assinante,
                tipo_tel,
                procon,
                operadora,
                flhot,
                flwhats,
            ),
        )


def inserir_nv_emails(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    emails = blocos.get("EMAILS", []) or []

    for e in emails:
        if not isinstance(e, dict):
            continue

        pos = to_int(e.get("POSICAO"))
        if pos is None:
            pos = 0

        email = e.get("EMAIL")
        if email in (None,):
            email = ""

        cur.execute(
            """
            INSERT INTO dbo.NV_Email (IdConsulta, Posicao, Email)
            VALUES (?, ?, ?)
            """,
            (id_consulta, pos, email),
        )


def inserir_nv_situacao_cadastral(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    sit = blocos.get("SITUACAOCADASTRAL", {}) or {}

    if not isinstance(sit, dict):
        return

    desc_ = sit.get("DESCRICAO")
    obs = sit.get("SITUACAO-CADASTRAL-OBSERVACOES")
    sit_esp = sit.get("SITUACAO-ESPECIAL")

    if desc_ is None and obs is None and sit_esp is None:
        return

    cur.execute(
        """
        INSERT INTO dbo.NV_SituacaoCadastral
            (IdConsulta, Descricao, Observacoes, SituacaoEspecial)
        VALUES (?, ?, ?, ?)
        """,
        (id_consulta, desc_, obs, sit_esp),
    )


def inserir_nv_qsa(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    qsa_blocos = blocos.get("QSA", []) or []

    if isinstance(qsa_blocos, dict):
        qsa_blocos = [qsa_blocos]
    elif isinstance(qsa_blocos, str):
        return

    for bloco in qsa_blocos:
        if not isinstance(bloco, dict):
            continue

        socios = bloco.get("QSA", []) or []
        if isinstance(socios, dict):
            socios = [socios]
        elif isinstance(socios, str):
            continue

        for s in socios:
            if not isinstance(s, dict):
                continue

            cur.execute(
                """
                INSERT INTO dbo.NV_QSA_Socio
                    (IdConsulta, Nome, Qualificacao, RendaSocio,
                     DDDSocio, CelSocio, FlWhats, CpfSocio,
                     CnpjEmpresa, RazaoEmpresa, Participacao,
                     DataFundacao, CNAE, DescricaoCNAE,
                     CodNatJur, DesNatureza, StatusRF)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    id_consulta,
                    s.get("NOME"),
                    s.get("QUALIFICACAO"),
                    s.get("RENDA_SOCIO"),
                    s.get("DDD_SOCIO"),
                    s.get("CEL_SOCIO"),
                    s.get("FLWHATS"),
                    s.get("CPF_SOCIO"),
                    s.get("CNPJ"),
                    s.get("RAZAO"),
                    s.get("PARTICIPACAO"),
                    s.get("DATA_FUNDACAO"),
                    s.get("CNAE"),
                    s.get("DESCRICAO_CNAE"),
                    s.get("COD_NATJUR"),
                    s.get("DES_NATUREZA"),
                    s.get("STATUS_RF"),
                ),
            )


def inserir_nv_perfilconsumo(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    perf = blocos.get("PERFILCONSUMO", {}) or {}

    if not perf:
        return

    consumo = perf.get("CONSUMO")
    persona = perf.get("PERSONADIGITAL")
    propen = to_int(perf.get("PROPENSAO_PAGAMENTO"))
    c6 = to_int(perf.get("CONSULTADOS_6MESES"))
    c12 = to_int(perf.get("CONSULTADOS_12MESES"))
    aposent = perf.get("POSSIVEL_APOSENTADO")

    cur.execute(
        """
        INSERT INTO dbo.NV_PerfilConsumo
            (IdConsulta, Consumo, PersonaDigital, PropenPagamento,
             Consultados6Meses, Consultados12Meses, PossivelAposentado)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (id_consulta, consumo, persona, propen, c6, c12, aposent),
    )


def inserir_nv_pessoas_ligadas(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    ligadas = blocos.get("PESSOASLIGADAS", []) or []

    for p in ligadas:
        if not isinstance(p, dict):
            continue

        cur.execute(
            """
            INSERT INTO dbo.NV_PessoaLigada
                (IdConsulta, Cpf, Nome, Vinculo, Nasc)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                id_consulta,
                p.get("CPF"),
                p.get("NOME"),
                p.get("VINCULO"),
                p.get("NASC"),
            ),
        )


def inserir_nv_sociedades_pf(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    sociedades = blocos.get("SOCIEDADES", []) or []

    for s in sociedades:
        if not isinstance(s, dict):
            continue

        cur.execute(
            """
            INSERT INTO dbo.NV_SociedadePF
                (IdConsulta, Cnpj, Razao, Participacao,
                 DataFundacao, CNAE, DescricaoCNAE,
                 CodNatJur, DesNatureza, StatusRF)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                id_consulta,
                s.get("CNPJ"),
                s.get("RAZAO"),
                s.get("PARTICIPACAO"),
                s.get("DATA_FUNDACAO"),
                s.get("CNAE"),
                s.get("DESCRICAO_CNAE"),
                s.get("COD_NATJUR"),
                s.get("DES_NATUREZA"),
                s.get("STATUS_RF"),
            ),
        )


def inserir_nv_pep(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    pep = blocos.get("PEP", {}) or {}
    if not isinstance(pep, dict):
        return

    flpep = pep.get("FLPEP")
    if flpep is None:
        return

    cur.execute(
        "INSERT INTO dbo.NV_PEP (IdConsulta, FlPep) VALUES (?, ?)",
        (id_consulta, flpep),
    )


def inserir_nv_pep_rel(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    rels = blocos.get("PEPRELACIONADOS", []) or []

    for r in rels:
        if not isinstance(r, dict):
            continue
        if not (r.get("CPF") or r.get("NOME") or r.get("VINCULO")):
            continue
        cur.execute(
            """
            INSERT INTO dbo.NV_PEP_Relacionado
                (IdConsulta, Cpf, Nome, Vinculo)
            VALUES (?, ?, ?, ?)
            """,
            (
                id_consulta,
                r.get("CPF"),
                r.get("NOME"),
                r.get("VINCULO"),
            ),
        )


def inserir_nv_obito(cur, id_consulta, resp):
    blocos = get_blocos(resp)
    obito = blocos.get("OBITO", {}) or {}
    if not isinstance(obito, dict):
        return

    flobito = obito.get("FLOBITO")
    if flobito is None:
        return
    cur.execute(
        "INSERT INTO dbo.NV_Obito (IdConsulta, FlObito) VALUES (?, ?)",
        (id_consulta, flobito),
    )


def main():
    try:
        print("Gerando token...")
        token = client.gerar_token()
        print("Token recebido (tratado):", token)
    except Exception as e:
        print("\n### ERRO AO GERAR TOKEN ###")
        print(e)
        return

    conn_sql = get_bdtelefones_connection()
    cur_sql = conn_sql.cursor()

    mysql_conn = get_mysql_connection()
    cur_mysql = mysql_conn.cursor()

    print("\nCarregando cache de NV_Geral (SQL Server)...")
    cur_sql.execute(
        """
        ;WITH Ultimas AS (
            SELECT
                Documento,
                IdConsulta,
                DataConsulta,
                ROW_NUMBER() OVER (
                    PARTITION BY Documento
                    ORDER BY DataConsulta DESC, IdConsulta DESC
                ) AS rn
            FROM dbo.NV_Geral
        )
        SELECT Documento,
               DataConsulta AS Ultima,
               IdConsulta
        FROM Ultimas
        WHERE rn = 1;
    """
    )
    rows_cache = cur_sql.fetchall()
    cache_ultima_consulta = {
        row[0]: {"data": row[1], "id_consulta": row[2]} for row in rows_cache
    }
    print(f"Cache carregado com {len(cache_ultima_consulta)} documentos já consultados.")

    print("\nCarregando documentos do GECOBI (MySQL)...")
    cur_mysql.execute(
        """
        SELECT DISTINCT cpfcnpj
        FROM cadastros_tb
        WHERE cod_cli IN (517,518,519)
          AND stcli <> 'INA'
          AND cpfcnpj IS NOT NULL
          AND TRIM(cpfcnpj) <> ''
        LIMIT 10000
    """
    )
    documentos = [row[0] for row in cur_mysql.fetchall()]
    print(f"Total de documentos carregados: {len(documentos)}")

    for documento in documentos:
        print("\n" + "=" * 60)
        print(f"Analisando necessidade de consulta para: {documento}")
        print("=" * 60)

        info_cache = cache_ultima_consulta.get(documento)

        if not deve_consultar_documento(documento, cache_ultima_consulta):
            print(f"Documento {documento} já possui consulta nos últimos 3 meses. Pulando.")
            continue

        if info_cache is None:
            print(f"Documento {documento} ainda não consultado. Vai INSERIR.")
        else:
            print(
                f"Documento {documento} consultado há mais de 3 meses. "
                f"Vai ATUALIZAR (nova linha em NV_Geral e atualizar CadastrosUnificados)."
            )

        print(f"Consultando NVCheck para: {documento}")

        try:
            resp = client.nvcheck(documento)

            id_consulta, tipo_pessoa = inserir_nv_geral(cur_sql, resp, documento)

            inserir_nv_enderecos(cur_sql, id_consulta, resp)
            inserir_nv_telefones(cur_sql, id_consulta, resp)
            inserir_nv_emails(cur_sql, id_consulta, resp)
            inserir_nv_situacao_cadastral(cur_sql, id_consulta, resp)

            if tipo_pessoa == "PJ":
                inserir_nv_qsa(cur_sql, id_consulta, resp)
            else:
                inserir_nv_perfilconsumo(cur_sql, id_consulta, resp)
                inserir_nv_pessoas_ligadas(cur_sql, id_consulta, resp)
                inserir_nv_sociedades_pf(cur_sql, id_consulta, resp)
                inserir_nv_pep(cur_sql, id_consulta, resp)
                inserir_nv_pep_rel(cur_sql, id_consulta, resp)
                inserir_nv_obito(cur_sql, id_consulta, resp)

            upsert_cadastro_unificado(cur_sql, resp, documento)

            conn_sql.commit()
            print(
                f"Consulta {id_consulta} salva/atualizada com sucesso para "
                f"{documento} ({tipo_pessoa})."
            )

            cache_ultima_consulta[documento] = {
                "data": datetime.now(),
                "id_consulta": id_consulta,
            }

        except Exception as e:
            print(f"\n### ERRO NA NVCheck / INSERT/UPDATE PARA {documento} ###")
            print(e)
            conn_sql.rollback()

    cur_sql.close()
    conn_sql.close()
    cur_mysql.close()
    mysql_conn.close()
    print("Processo finalizado.")


if __name__ == "__main__":
    main()
