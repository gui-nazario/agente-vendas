import os
import json
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text


# ----------------------------
# Helpers
# ----------------------------

def preparar_df(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza tipos e remove datas inválidas."""
    df = df.copy()

    # Garante date (AAAA-MM-DD)
    df["data_venda"] = pd.to_datetime(df["data_venda"], errors="coerce").dt.date
    df = df.dropna(subset=["data_venda"])

    # valor_total pode vir Decimal/str
    if "valor_total" in df.columns:
        df["valor_total"] = pd.to_numeric(df["valor_total"], errors="coerce").fillna(0.0)

    return df


def serie_por_dia_completo(serie_por_dia: pd.Series) -> pd.Series:
    """Remove o dia atual se for o último (provável parcial)."""
    if serie_por_dia.empty:
        return serie_por_dia

    hoje = date.today()
    if serie_por_dia.index[-1] == hoje:
        print(f"DEBUG: Ignorando o dia atual ({hoje}) por ser parcial.")
        return serie_por_dia.iloc[:-1]

    return serie_por_dia


# ----------------------------
# Detectores
# ----------------------------

def detectar_queda_faturamento(df: pd.DataFrame, queda_pct: float = 0.30):
    """
    Queda de faturamento: compara último dia COMPLETO vs dia anterior.
    """
    if df.empty:
        print("DEBUG: df está vazio (sem dados na tabela vendas).")
        return None

    por_dia = df.groupby("data_venda")["valor_total"].sum().sort_index()

    print("\nDEBUG - Faturamento por dia (últimos 5):")
    print(por_dia.tail(5))

    por_dia = serie_por_dia_completo(por_dia)

    if len(por_dia) < 2:
        print("DEBUG: Não há pelo menos 2 dias completos para comparação.")
        return None

    ultimo_dia = por_dia.index[-1]
    dia_anterior = por_dia.index[-2]

    faturamento_ultimo = float(por_dia.iloc[-1])
    faturamento_anterior = float(por_dia.iloc[-2])

    print("\nDEBUG - Comparando faturamento (dias completos):")
    print(f"Dia anterior ({dia_anterior}): {faturamento_anterior:.2f}")
    print(f"Último dia   ({ultimo_dia}): {faturamento_ultimo:.2f}")

    if faturamento_anterior == 0:
        print("DEBUG: Faturamento do dia anterior é 0, não dá pra calcular variação.")
        return None

    variacao = (faturamento_ultimo - faturamento_anterior) / faturamento_anterior
    print(f"DEBUG - Variação faturamento (%): {variacao * 100:.2f}%")

    if variacao <= -queda_pct:
        return {
            "tipo": "queda_faturamento",
            "severidade": "alta",
            "detalhe": f"Queda de {abs(variacao) * 100:.2f}% no faturamento do dia {ultimo_dia}",
            "contexto": {
                "dia_anterior": str(dia_anterior),
                "ultimo_dia": str(ultimo_dia),
                "faturamento_anterior": faturamento_anterior,
                "faturamento_ultimo": faturamento_ultimo,
                "variacao_pct": float(variacao * 100),
                "queda_pct_configurada": float(queda_pct * 100),
            },
        }

    return None


def detectar_faturamento_muito_baixo(df: pd.DataFrame, limite: float = 10.0):
    if df.empty:
        return None

    por_dia = df.groupby("data_venda")["valor_total"].sum().sort_index()

    hoje = date.today()
    if por_dia.index[-1] == hoje:
        por_dia = por_dia.iloc[:-1]

    if por_dia.empty:
        return None

    ultimo_dia = por_dia.index[-1]
    total = float(por_dia.iloc[-1])

    print(f"DEBUG - Faturamento último dia completo ({ultimo_dia}): {total:.2f}")

    if total <= limite:
        return {
            "tipo": "faturamento_muito_baixo",
            "severidade": "alta",
            "detalhe": f"Faturamento muito baixo (≤ R$ {limite:.2f}) no dia {ultimo_dia}",
            "contexto": {
                "dia": str(ultimo_dia),
                "faturamento_total": total,
                "limite_configurado": limite
            }
        }

    return None

def detectar_queda_numero_vendas(df: pd.DataFrame, queda_pct: float = 0.30):
    """
    Queda no volume de vendas: compara contagem de registros do último dia COMPLETO vs anterior.
    """
    if df.empty:
        return None

    # contagem por dia (cada linha = uma venda)
    por_dia = df.groupby("data_venda")["id"].count().sort_index()

    print("\nDEBUG - Número de vendas por dia (últimos 5):")
    print(por_dia.tail(5))

    por_dia = serie_por_dia_completo(por_dia)

    if len(por_dia) < 2:
        return None

    ultimo_dia = por_dia.index[-1]
    dia_anterior = por_dia.index[-2]

    vendas_ultimo = int(por_dia.iloc[-1])
    vendas_anterior = int(por_dia.iloc[-2])

    print("\nDEBUG - Comparando número de vendas (dias completos):")
    print(f"Dia anterior ({dia_anterior}): {vendas_anterior}")
    print(f"Último dia   ({ultimo_dia}): {vendas_ultimo}")

    if vendas_anterior == 0:
        return None

    variacao = (vendas_ultimo - vendas_anterior) / vendas_anterior
    print(f"DEBUG - Variação número de vendas (%): {variacao * 100:.2f}%")

    if variacao <= -queda_pct:
        severidade = "alta" if abs(variacao) >= 0.60 else "media"
        return {
            "tipo": "queda_numero_vendas",
            "severidade": severidade,
            "detalhe": f"Queda de {abs(variacao) * 100:.2f}% no número de vendas no dia {ultimo_dia}",
            "contexto": {
                "dia_anterior": str(dia_anterior),
                "ultimo_dia": str(ultimo_dia),
                "vendas_anterior": vendas_anterior,
                "vendas_ultimo": vendas_ultimo,
                "variacao_pct": float(variacao * 100),
                "queda_pct_configurada": float(queda_pct * 100),
            },
        }

    return None


def detectar_possivel_fraude_duplicidade(df: pd.DataFrame, limite_repeticoes: int = 3):
    """
    Suspeita de fraude/duplicidade:
    Mesmo cliente + mesmo valor_total + mesmo dia repetindo >= limite_repeticoes.
    (Sem order_id/timestamp, é uma heurística por dia.)
    """
    if df.empty:
        return None

    # Agrupa por dia, cliente e valor_total
    agrupado = (
        df.groupby(["data_venda", "cliente", "valor_total"])
        .size()
        .reset_index(name="repeticoes")
        .sort_values(["repeticoes"], ascending=False)
    )

    suspeitos = agrupado[agrupado["repeticoes"] >= limite_repeticoes]

    print("\nDEBUG - Top suspeitas de duplicidade (até 5):")
    print(agrupado.head(5))

    if suspeitos.empty:
        return None

    # Pega o caso mais forte
    row = suspeitos.iloc[0]

    return {
        "tipo": "possivel_fraude_duplicidade",
        "severidade": "alta",
        "detalhe": (
            f"Possível fraude/duplicidade: cliente '{row['cliente']}' repetiu compra de "
            f"{float(row['valor_total']):.2f} {int(row['repeticoes'])}x no dia {row['data_venda']}. "
            f"Sugerir análise antifraude."
        ),
        "contexto": {
            "data_venda": str(row["data_venda"]),
            "cliente": str(row["cliente"]),
            "valor_total": float(row["valor_total"]),
            "repeticoes": int(row["repeticoes"]),
            "limite_repeticoes": int(limite_repeticoes),
        },
    }


# ----------------------------
# Persistência
# ----------------------------

def registrar_incidente(engine, alerta: dict):
    """
    Insere um registro na tabela incidentes.
    Espera tabela: incidentes(tipo text, severidade text, detalhe text, contexto jsonb)
    """
    sql = text("""
        INSERT INTO incidentes (tipo, severidade, detalhe, contexto)
        VALUES (:tipo, :severidade, :detalhe, CAST(:contexto AS jsonb))
    """)

    params = {
        "tipo": alerta["tipo"],
        "severidade": alerta["severidade"],
        "detalhe": alerta["detalhe"],
        "contexto": json.dumps(alerta.get("contexto", {}), ensure_ascii=False),
    }

    with engine.begin() as conn:
        conn.execute(sql, params)


# ----------------------------
# Main
# ----------------------------

def main():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL não encontrada (configure no GitHub Secrets ou no seu ambiente local).")

    engine = create_engine(db_url)

    # Puxa colunas necessárias para todos os detectores
    df = pd.read_sql(
        "SELECT id, data_venda, cliente, valor_total FROM vendas;",
        engine
    )

    df = preparar_df(df)

    alertas = []

    # Rodar todos os cenários
    for detector in [
        lambda d: detectar_vendas_zeradas(d),
        lambda d: detectar_queda_faturamento(d, queda_pct=0.30),
        lambda d: detectar_queda_numero_vendas(d, queda_pct=0.30),
        lambda d: detectar_possivel_fraude_duplicidade(d, limite_repeticoes=3),
    ]:
        alerta = detector(df)
        if alerta:
            alertas.append(alerta)

    if alertas:
        print(f"\n🚨 {len(alertas)} ALERTA(S) DETECTADO(S)")
        for a in alertas:
            print(f"- [{a['tipo']}] {a['detalhe']}")
            registrar_incidente(engine, a)
        print("✅ Incidente(s) registrado(s) na tabela incidentes.")
    else:
        total_geral = float(df["valor_total"].sum()) if not df.empty else 0.0
        print(f"✅ OK: nenhum incidente detectado. Total geral (base inteira) = {total_geral:.2f}")


if __name__ == "__main__":
    main()
