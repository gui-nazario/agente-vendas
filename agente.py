import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


def registrar_incidente(engine, tipo: str, severidade: str, detalhe: str, contexto: dict | None = None):
    """Grava incidente na tabela incidentes (para histórico e Power BI)."""
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


def detectar_queda_ultimo_dia(df: pd.DataFrame, queda_pct: float = 0.30) -> dict | None:
    """
    Compara faturamento do último dia com o dia anterior.
    Se cair >= queda_pct (ex: 0.30 = 30%), retorna um alerta (dict). Senão retorna None.
    """
    if df.empty:
        return {
            "tipo": "SEM_DADOS",
            "severidade": "ALTA",
            "detalhe": "Tabela vendas está vazia.",
            "contexto": {}
        }

    df2 = df.copy()
    df2["data_venda"] = pd.to_datetime(df2["data_venda"])

    # Soma faturamento por dia
    por_dia = df2.groupby(df2["data_venda"].dt.date)["valor_total"].sum().sort_index()

    if len(por_dia) < 2:
        return {
            "tipo": "POUCOS_DIAS",
            "severidade": "MEDIA",
            "detalhe": "Não há dias suficientes para comparar (precisa de pelo menos 2 dias).",
            "contexto": {"dias_disponiveis": int(len(por_dia))}
        }

    ultimo_dia = por_dia.index[-1]
    dia_anterior = por_dia.index[-2]

    fat_ultimo = float(por_dia.loc[ultimo_dia])
    fat_ant = float(por_dia.loc[dia_anterior])

    # evita divisão por zero
    if fat_ant == 0:
        return {
            "tipo": "DIA_ANTERIOR_ZERO",
            "severidade": "MEDIA",
            "detalhe": f"Dia anterior ({dia_anterior}) teve faturamento 0, não dá para calcular queda percentual.",
            "contexto": {"dia_anterior": str(dia_anterior), "faturamento_anterior": fat_ant}
        }

    variacao = (fat_ultimo - fat_ant) / fat_ant  # negativo = caiu
    queda = -variacao  # positivo quando caiu

    # Ex: queda_pct=0.30 => alerta se queda >= 30%
    if queda >= queda_pct:
        return {
            "tipo": "QUEDA_FATURAMENTO_DIA",
            "severidade": "ALTA",
            "detalhe": (
                f"Queda de {queda*100:.1f}% no faturamento: "
                f"{dia_anterior}={fat_ant:.2f} -> {ultimo_dia}={fat_ultimo:.2f}"
            ),
            "contexto": {
                "dia_anterior": str(dia_anterior),
                "ultimo_dia": str(ultimo_dia),
                "faturamento_anterior": fat_ant,
                "faturamento_ultimo": fat_ultimo,
                "variacao_pct": variacao,
                "queda_pct": queda
            }
        }

    return None


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL não configurada nos Secrets do GitHub.")

    engine = create_engine(db_url)

    # Puxa só o que precisa (melhor performance)
    df = pd.read_sql("""
        SELECT data_venda, valor_total
        FROM vendas
    """, engine)

    alerta = detectar_queda_ultimo_dia(df, queda_pct=0.30)

    if alerta:
        print("🚨 ALERTA:", alerta["detalhe"])
        registrar_incidente(
            engine=engine,
            tipo=alerta["tipo"],
            severidade=alerta["severidade"],
            detalhe=alerta["detalhe"],
            contexto=alerta["contexto"]
        )
    else:
        print("✅ OK: sem queda relevante no último dia.")


if __name__ == "__main__":
    main()
