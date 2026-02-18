# 🧠 Agente de Monitoramento de Vendas

Sistema automatizado para detecção de anomalias em vendas utilizando **Python, PostgreSQL e regras de negócio**.

O agente analisa os dados diariamente, identifica comportamentos suspeitos e registra incidentes automaticamente.

---

## 🎯 Problema que Resolve

Empresas precisam identificar rapidamente:

- 📉 Queda abrupta de faturamento  
- 📊 Redução significativa no volume de vendas  
- 🚨 Faturamento anormalmente baixo  
- 🔍 Possível fraude por duplicidade de compras  

Este agente automatiza essa análise.

---

## ⚙️ Como Funciona

1. Consulta dados da tabela `vendas`
2. Converte os dados para DataFrame (Pandas)
3. Executa detectores de anomalia
4. Aplica lógica de prioridade
5. Registra incidentes na tabela `incidentes`

---

## 🔍 Cenários Detectados

### • Faturamento Muito Baixo
Dispara quando o último dia completo tem faturamento ≤ R$ 10.

### • Queda de Faturamento
Compara último dia completo com o anterior.  
Dispara se queda ≥ 30%.

### • Queda no Número de Vendas
Compara volume diário de vendas.  
Dispara se queda ≥ 30%.

### • Possível Fraude / Duplicidade
Detecta cliente que repetiu a mesma compra (mesmo valor no mesmo dia) 3 ou mais vezes.

---

## 🏗 Arquitetura

PostgreSQL → SQLAlchemy → Pandas → Engine de Regras → Registro de Incidentes (JSONB)

---

## 🚀 Como Executar

### 1️⃣ Clonar o repositório

```bash
git clone https://github.com/SEU-USUARIO/agente-vendas.git
cd agente-vendas
````

###  2️⃣ Instalar dependências
````bash
pip install -r requirements.txt
````
3️⃣ Configurar variável de ambiente
export DATABASE_URL="postgresql+psycopg2://USER:SENHA@HOST:PORT/DB"

(No Windows PowerShell:)
setx DATABASE_URL "postgresql+psycopg2://USER:SENHA@HOST:PORT/DB"

### 4️⃣ Executar

```bash
python agente.py

