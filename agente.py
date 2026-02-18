import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


def detectar_queda_ultimo_dia(df, queda_pct=0.30):
    # Agrupa faturamento por dia
    por_dia = (
        df.groupby("data_venda")["valor_total"]
        .sum()
        .sort_index()
    )

    if len(por_dia) < 2:
        return None

    ultimo_dia = por_dia.index[-1]
    dia_anterior = por_dia.index[-2]

    faturamento_ultimo = por_dia.iloc[-1]
    faturamento_anterior = por_dia.iloc[-2]

    if faturamento_anterior == 0:
        return None

    variacao = (faturamento_ultimo - faturamento_anterior) / faturamento_anterior

    if variacao <= -queda_pct:
        return {
            "tipo": "queda_faturamento",
            "severidade": "alta",
            "detalhe": f"Queda de {abs(variacao)*100:.2f}% no dia {ultimo_dia}",
            "contexto": {
                "ultimo_dia": str(ultimo_dia),
                "faturamento_ultimo": float(faturamento_ultimo),
                "faturamento_anterior": float(faturamento_anterior),
                "variacao_pct": float(variacao * 100),
            },
        }

    return None


def registrar_incidente(engine, alerta):
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
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        raise ValueError("DATABASE_URL não encontrada")

    engine = create_engine(db_url)

    df = pd.read_sql("SELECT data_venda, valor_total FROM vendas;", engine)

    alerta = detectar_queda_ultimo_dia(df, queda_pct=0.30)

    if alerta:
        print("🚨 ALERTA DETECTADO")
        print(alerta["detalhe"])
        registrar_incidente(engine, alerta)
        print("✅ Incidente registrado.")
    else:
        total = df["valor_total"].sum()
        print(f"OK: faturamento normal ({total:.2f}).")


if __name__ == "__main__":
    main()
