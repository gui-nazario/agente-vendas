import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


def registrar_incidente(engine, tipo: str, severidade: str, detalhe: str, contexto: dict | None = None):
    """
    Insere um incidente no banco para você ter histórico (e usar no Power BI).
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO incidentes (tipo, severidade, detalhe, contexto)
                VALUES (:tipo, :severidade, :detalhe, :contexto::jsonb)
            """),
            {
                "tipo": tipo,
                "severidade": severidade,
                "detalhe": detalhe,
                "contexto": json.dumps(contexto or {})
            }
        )


def detectar_queda_ultimo_dia(df: pd.DataFrame, queda_minima_pct: float = 30.0) -> dict | None:
    """
    Compara o faturamento do último dia com o dia anterior.
    Se a queda (%) for >= queda_minima_pct, retorna um dict com os dados do alerta.
    Senão retorna None.
    """
    if df.empty:
        return {
            "tipo": "SEM_DADOS",
            "severidade": "ALTA",
            "detalhe": "Tabela vendas está vazia.",
            "contexto": {}
        }

    # garante tipo datetime
    df2 = df.copy()
    df2["data_venda"] = pd.to_datetime(df2["data_venda"])

    # soma por dia
    por_dia = (
        df2.groupby(df2["data_venda"].dt.date)["valor_total"]
        .sum()
        .sort_index()
    )

    if len(por_dia) < 2:
        return {
            "tipo": "POUCOS_DIAS",
            "severidade": "MEDIA",
            "detalhe": "Não há dias suficientes para comparar (precisa de pelo menos 2 dias).",
            "contexto": {"dias_disponiveis": int(len(por_dia))}
        }

    dia_anterior = por_dia.index[-2]
    ultimo_dia = por_dia.index[-1]

    faturamento_anterior = float(por_dia.loc[dia_anterior])
    faturamento_ultimo = float(por_dia.loc[ultimo_dia])

    if faturamento_anterior == 0:
        return {
            "tipo": "DIA_ANTERIOR_ZERO",
            "severidade": "MEDIA",
            "detalhe": f"Dia anterior ({dia_anterior}) teve faturamento 0, não dá para calcular queda.",
            "contexto": {
                "dia_anterior": str(dia_anterior),
                "faturamento_anterior": faturamento_anterior
            }
        }

    queda_pct = ((faturamento_anterior - faturamento_ultimo) / faturamento_anterior) * 100.0

    # logs úteis no GitHub Actions
    print(f"📅 Dia anterior ({dia_anterior}): {faturamento_anterior:.2f}")
    print(f"📅 Último dia ({ultimo_dia}): {faturamento_ultimo:.2f}")
    print(f"📉 Queda calculada: {queda_pct:.2f}% (limite: {queda_minima_pct:.2f}%)")

    if queda_pct >= queda_minima_pct:
        return {
            "tipo": "QUEDA_FATURAMENTO_DIA",
            "severidade": "ALTA",
            "detalhe": (
                f"Queda de {queda_pct:.2f}% no faturamento: "
                f"{dia_anterior}={faturamento_anterior:.2f} -> {ultimo_dia}={faturamento_ultimo:.2f}"
            ),
            "contexto": {
                "dia_anterior": str(dia_anterior),
                "ultimo_dia": str(ultimo_dia),
                "faturamento_anterior": faturamento_anterior,
                "faturamento_ultimo": faturamento_ultimo,
                "queda_pct": queda_pct,
                "limite_pct": queda_minima_pct
            }
        }

    return None


def main():
    # 1) Pega a URL do banco via Secret do GitHub
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL não configurada nos Secrets do GitHub.")

    # 2) Conecta no Neon
    engine = create_engine(db_url)

    # 3) Busca somente colunas necessárias
    df = pd.read_sql(
        "SELECT data_venda, valor_total FROM vendas;",
        engine
    )

    # 4) Roda o agente de queda diária
    alerta = detectar_queda_ultimo_dia(df, queda_minima_pct=30.0)

    if alerta:
        print("🚨 ALERTA DISPARADO:", alerta["detalhe"])
        registrar_incidente(
            engine,
            tipo=alerta["tipo"],
            severidade=alerta["severidade"],
            detalhe=alerta["detalhe"],
            contexto=alerta["contexto"]
        )
        print("📝 Incidente registrado na tabela incidentes.")
    else:
        print("✅ OK: sem queda relevante no último dia.")


if __name__ == "__main__":
    main()
