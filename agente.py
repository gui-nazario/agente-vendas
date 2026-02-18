import os
import json
import pandas as pd
from sqlalchemy import create_engine, text


def registrar_incidente(engine, tipo, severidade, detalhe, contexto=None):
    """
    Salva um incidente na tabela 'incidentes' do seu Neon.
    Isso cria histórico para você ver no Power BI depois.
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
    # 1) Pega a URL do banco de uma variável de ambiente (GitHub Secret)
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL não configurada nos Secrets do GitHub.")

    # 2) Cria conexão com o Neon
    engine = create_engine(db_url)

    # 3) Lê vendas do banco
    df = pd.read_sql("SELECT * FROM vendas;", engine)

    # 4) Faz uma regra simples: se faturamento total estiver baixo, gera alerta
    faturamento = float(df["valor_total"].sum())

    limite = 100000.0  # você pode ajustar depois
    if faturamento < limite:
        detalhe = f"Faturamento baixo: {faturamento:.2f} (limite {limite:.2f})"
        print("🚨 ALERTA:", detalhe)

        # 5) Registra incidente no banco
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
