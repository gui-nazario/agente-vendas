import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


def detectar_queda_ultimo_dia(df, queda_pct=0.30):
    if df.empty:
        print("DEBUG: df está vazio")
        return None

    df = df.copy()
    df["data_venda"] = pd.to_datetime(df["data_venda"])

    por_dia = (
        df.groupby(df["data_venda"].dt.date)["valor_total"]
        .sum()
        .sort_index()
    )

    print("\nDEBUG - Faturamento por dia (últimos 5):")
    print(por_dia.tail(5))

    if len(por_dia) < 2:
        return None

    ultimo_dia = por_dia.index[-1]
    dia_anterior = por_dia.index[-2]

    faturamento_ultimo = float(por_dia.iloc[-1])
    faturamento_anterior = float(por_dia.iloc[-2])

    if faturamento_anterior == 0:
        return None

    variacao = (faturamento_ultimo - faturamento_anterior) / faturamento_anterior

    print("\nDEBUG - Comparando:")
    print("Dia anterior:", dia_anterior, faturamento_anterior)
    print("Último dia   :", ultimo_dia, faturamento_ultimo)
    print("Variação (%):", variacao * 100)

    if variacao <= -queda_pct:
        return {
            "tipo": "queda_faturamento",
            "severidade": "alta",
            "detalhe": f"Queda de {abs(variacao)*100:.2f}% no dia {ultimo_dia}",
            "contexto": {
                "dia_anterior": str(dia_anterior),
                "ultimo_dia": str(ultimo_dia),
                "faturamento_anterior": faturamento_anterior,
                "faturamento_ultimo": faturamento_ultimo,
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
        total = float(df["valor_total"].sum())
        print(f"✅ OK: sem queda relevante. Total geral={total:.2f}")


if __name__ == "__main__":
    main()
