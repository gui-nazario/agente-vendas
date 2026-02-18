import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


def registrar_incidente(engine, tipo: str, severidade: str, detalhe: str, contexto: dict | None = None):
    """
    Salva um incidente na tabela 'incidentes' do Neon.
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


def main():
    # Lê a conexão do Secret do GitHub
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL não configurada nos Secrets do GitHub.")

    engine = create_engine(db_url)

    # Puxa as vendas
    df = pd.read_sql("SELECT * FROM vendas;", engine)

    # Regra simples (você ajusta depois)
    faturamento = float(df["valor_total"].sum())
    limite = 100000.0

    if faturamento < limite:
        detalhe = f"Faturamento baixo: {faturamento:.2f} (limite {limite:.2f})"
        print("🚨 ALERTA:", detalhe)

        registrar_incidente(
            engine,
            tipo="FATURAMENTO_BAIXO",
            severidade="ALTA",
            detalhe=detalhe,
            contexto={"faturamento": faturamento, "limite": limite}
        )
    else:
        print(f"✅ OK: faturamento normal ({faturamento:.2f}).")


if __name__ == "__main__":
    main()
