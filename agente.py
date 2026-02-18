import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


def detectar_queda_ultimo_dia(df: pd.DataFrame, queda_pct: float = 0.30):
    """
    Compara o faturamento do último dia com o dia anterior.
    Se a queda for maior/igual a queda_pct (ex: 0.30 = 30%), retorna um "alerta" (dict).
    Se não houver queda relevante, retorna None.

    df precisa ter: data_venda, valor_total
    """

    # 0) Se não tem dados, não tem o que analisar
    if df.empty:
        print("DEBUG: df está vazio (sem dados na tabela vendas).")
        return None

    # 1) Garante que data_venda seja datetime (e depois vira date AAAA-MM-DD)
    df = df.copy()
    df["data_venda"] = pd.to_datetime(df["data_venda"], errors="coerce").dt.date

    # Se alguma data veio inválida (NaT), remove
    df = df.dropna(subset=["data_venda"])

    # 2) Soma faturamento por dia
    por_dia = (
        df.groupby("data_venda")["valor_total"]
        .sum()
        .sort_index()
    )

    print("\nDEBUG - Faturamento por dia (últimos 5):")
    print(por_dia.tail(5))

    # 3) Precisa de pelo menos 2 dias pra comparar
    if len(por_dia) < 2:
        print("DEBUG: Não há pelo menos 2 dias para comparação.")
        return None

    ultimo_dia = por_dia.index[-1]
    dia_anterior = por_dia.index[-2]

    faturamento_ultimo = float(por_dia.iloc[-1])
    faturamento_anterior = float(por_dia.iloc[-2])

    print("\nDEBUG - Comparando últimos 2 dias:")
    print(f"Dia anterior ({dia_anterior}): {faturamento_anterior:.2f}")
    print(f"Último dia   ({ultimo_dia}): {faturamento_ultimo:.2f}")

    # 4) Evita divisão por zero
    if faturamento_anterior == 0:
        print("DEBUG: Faturamento do dia anterior é 0, não dá pra calcular variação.")
        return None

    # 5) Calcula variação (negativo = queda)
    variacao = (faturamento_ultimo - faturamento_anterior) / faturamento_anterior
    print(f"DEBUG - Variação (%): {variacao * 100:.2f}%")

    # 6) Se caiu mais que o limite, gera alerta
    if variacao <= -queda_pct:
        return {
            "tipo": "queda_faturamento",
            "severidade": "alta",
            "detalhe": f"Queda de {abs(variacao) * 100:.2f}% no dia {ultimo_dia}",
            "contexto": {
                "dia_anterior": str(dia_anterior),
                "ultimo_dia": str(ultimo_dia),
                "faturamento_anterior": faturamento_anterior,
                "faturamento_ultimo": faturamento_ultimo,
                "variacao_pct": float(variacao * 100),
                "queda_pct_configurada": float(queda_pct * 100),
            },
        }

    # 7) Sem queda relevante
    return None


def registrar_incidente(engine, alerta: dict):
    """
    Insere um registro na tabela incidentes.
    Espera tabela: incidentes(tipo text, severidade text, detalhe text, contexto jsonb)
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO incidentes (tipo, severidade, detalhe, contexto)
                VALUES (:tipo, :severidade, :detalhe, :contexto::jsonb)
            """),
            {
                "tipo": alerta["tipo"],
                "severidade": alerta["severidade"],
                "detalhe": alerta["detalhe"],
                "contexto": json.dumps(alerta["contexto"]),
            },
        )


def main():
    """
    Fluxo do agente:
    1) Pega DATABASE_URL do ambiente (GitHub Secrets ou sua máquina).
    2) Conecta no Postgres (Neon).
    3) Lê vendas (data_venda, valor_total).
    4) Detecta queda no último dia.
    5) Se tiver alerta, registra em incidentes.
    """

    # 1) URL do banco via variável de ambiente (GitHub Secret)
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL não encontrada (configure no GitHub Secrets ou no seu ambiente local).")

    # 2) Engine (conexão)
    engine = create_engine(db_url)

    # 3) Puxa só o que precisa para a análise
    df = pd.read_sql("SELECT data_venda, valor_total FROM vendas;", engine)

    # 4) Detecta queda
    alerta = detectar_queda_ultimo_dia(df, queda_pct=0.30)

    # 5) Decide o que fazer
    if alerta:
        print("🚨 ALERTA DETECTADO")
        print(alerta["detalhe"])
        registrar_incidente(engine, alerta)
        print("✅ Incidente registrado na tabela incidentes.")
    else:
        total_geral = float(df["valor_total"].sum())
        print(f"✅ OK: sem queda relevante. Total geral (base inteira) = {total_geral:.2f}")


if __name__ == "__main__":
    main()
