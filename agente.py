import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


def detectar_queda_ultimo_dia(df: pd.DataFrame, queda_pct: float = 0.30):
    """
    Compara faturamento do último dia com o dia anterior.
    queda_pct = 0.30 significa 30% de queda.
    Retorna um dict de alerta ou None.
    """

    if df.empty:
        print("DEBUG: df está vazio.")
        return None

    # 1) GARANTE que data_venda é data mesmo (não string)
    df = df.copy()
    df["data_venda"] = pd.to_datetime(df["data_venda"]).dt.date  # fica só YYYY-MM-DD

    # 2) Soma faturamento por dia
    por_dia = (
        df.groupby("data_venda")["valor_total"]
        .sum()
        .sort_index()
    )

    print("\nDEBUG - Faturamento por dia (últimos 5):")
    print(por_dia.tail(5))

    if len(por_dia) < 2:
        print("DEBUG: menos de 2 dias para comparar.")
        return None

    # 3) Pega último e penúltimo dia
    ultimo_dia = por_dia.index[-1]
    dia_anterior = por_dia.index[-2]

    faturamento_ultimo = float(por_dia.loc[ultimo_dia])
    faturamento_anterior = float(por_dia.loc[dia_anterior])

    print("\nDEBUG - Comparando dias:")
    print("Dia anterior:", dia_anterior, "Faturamento:", faturamento_anterior)
    print("Último dia   :", ultimo_dia, "Faturamento:", faturamento_ultimo)

    if faturamento_anterior == 0:
        print("DEBUG: faturamento do dia anterior é 0, não dá pra calcular queda.")
        return None

    # 4) Calcula variação (negativa = queda)
    variacao = (faturamento_ultimo - faturamento_anterior) / faturamento_anterior
    queda = -variacao  # positivo quando cai

    print("DEBUG - Variação:", variacao)
    print("DEBUG - Queda (%):", queda * 100)

    # 5) Dispara alerta se queda >= limite
    if queda >= queda_pct:
        return {
            "tipo": "QUEDA_FATURAMENTO_DIA",
            "severidade": "ALTA",
            "detalhe": (
                f"Queda de {queda*100:.2f}% no faturamento: "
                f"{dia_anterior}={faturamento_anterior:.2f} -> {ultimo_dia}={faturamento_ultimo:.2f}"
            ),
            "contexto": {
                "dia_anterior": str(dia_anterior),
                "ultimo_dia": str(ultimo_dia),
                "faturamento_anterior": faturamento_anterior,
                "faturamento_ultimo": faturamento_ultimo,
                "variacao_pct": variacao * 100,
                "queda_pct": queda * 100,
                "limite_queda_pct": queda_pct * 100,
            },
        }

    return None


def registrar_incidente(engine, alerta: dict):
    """Salva o incidente na tabela incidentes."""
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
        raise ValueError("DATABASE_URL não encontrada (GitHub Secret).")

    engine = create_engine(db_url)

    # Puxa dados
    df = pd.read_sql("SELECT data_venda, valor_total FROM vendas;", engine)

    # Roda detecção
    alerta = detectar_queda_ultimo_dia(df, queda_pct=0.30)

    if alerta:
        print("\n🚨 ALERTA DETECTADO")
        print(alerta["detalhe"])
        registrar_incidente(engine, alerta)
        print("✅ Incidente registrado na tabela incidentes.")
    else:
        total = float(df["valor_total"].sum())
        print(f"\n✅ OK: sem queda relevante no último dia. (total geral={total:.2f})")


if __name__ == "__main__":
    main()
