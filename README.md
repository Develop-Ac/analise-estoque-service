# Análise Estoque Service

Este serviço é responsável pelo processamento pesado e análise de estoque da AC Acessórios. Ele executa periodicamente (semanalmente) cálculos de FIFO, curva ABC, tendências de venda e sugestões de compra, persistindo os resultados no banco de dados PostgreSQL.

## 🚀 Funcionalidades

1.  **Cálculo FIFO**: Determina o valor real do estoque e a idade dos produtos com base nas entradas e saídas.
2.  **Curva ABC**: Classifica os produtos em A, B, C ou D com base no valor vendido acumulado.
3.  **Análise de Tendência**:
    *   Calcula a tendência de vendas comparando períodos atuais vs. anteriores (12 meses, 6 meses, 90 dias).
    *   Aplica pesos para gerar um fator de tendência: 20% (12m), 50% (6m), 30% (90d).
4.  **Detecção de Ruptura**: Estima dias sem estoque nos últimos 2 anos para ajustar a demanda média diária.
5.  **Sugestão de Estoque (Mín/Máx)**:
    *   Calcula os níveis ideais de estoque baseados na Curva ABC e no Lead Time.
    *   Ajusta os níveis com base no Fator de Tendência.
6.  **Detecção de Alterações**:
    *   Compara a análise atual com a última gravada no banco.
    *   Marca produtos que tiveram mudança de **Curva ABC**, **Estoque Mínimo** ou **Estoque Máximo** (coluna `teve_alteracao_analise`).
7.  **Persistência**: Salva os resultados detalhados na tabela `com_fifo_completo` no PostgreSQL.

## 🛠️ Tecnologias

*   **Linguagem**: Python 3.11
*   **Banco de Dados**: PostgreSQL (Armazenamento), SQL Server (Origem/ERP via FreeTDS)
*   **Bibliotecas**: Pandas, NumPy, SQLAlchemy, PyODBC.

## ⚙️ Configuração (Variáveis de Ambiente)

O serviço é configurado via variáveis de ambiente. Defina estas variáveis no seu `docker-compose.yml` ou painel de controle (EasyPanel).

| Variável | Descrição | Padrão |
|Data|---|---|
| `INTERVALO_DIAS` | Intervalo entre execuções do job (em dias) | `7` |
| `POSTGRES_URL` | String de conexão SQLAlchemy para o PostgreSQL | (Obrigatório) |
| `SQL_HOST` | Host do SQL Server (ERP) | `192.168.1.146` |
| `SQL_PORT` | Porta do SQL Server | `1433` |
| `SQL_DATABASE` | Database do SQL Server | `master` |
| `SQL_USER` | Usuário do SQL Server | `USER_CONSULTA` |
| `SQL_PASSWORD` | Senha do SQL Server | `Ac@2025acesso` |
| `PYTHONUNBUFFERED`| Define como `1` para logs em tempo real no Docker | `1` |

### Exemplo de `.env` ou Environment Variables:
```ini
INTERVALO_DIAS=7
POSTGRES_URL=postgresql://user:pass@host:5432/db
SQL_HOST=192.168.1.146
SQL_USER=sa
SQL_PASSWORD=secret
```

## 📦 Como Rodar

### Docker (Recomendado)

1.  **Build da Imagem**:
    ```bash
    docker build -t analise-estoque-worker .
    ```

2.  **Rodar via Docker Compose**:
    ```bash
    docker-compose up -d
    ```

### Localmente (Desenvolvimento)

1.  Instale as dependências de sistema (FreeTDS para Linux/Mac ou Driver ODBC Driver 17/18 for SQL Server para Windows).
2.  Instale as bibliotecas Python:
    ```bash
    pip install -r requirements.txt
    ```
3.  Execute o script:
    ```bash
    # Executar o serviço (loop)
    python main.py
    
    # Ou forçar uma execução única
    python main.py run
    
    # Apenas criar tabelas
    python main.py create
    ```

## 🗄️ Banco de Dados (Schema)

O serviço cria e mantém a tabela `com_fifo_completo` no PostgreSQL.

**Colunas Principais:**
*   `pro_codigo`: Código do produto.
*   `curva_abc`: Classificação A, B, C, D.
*   `estoque_min_sugerido`: Nível mínimo calculado.
*   `estoque_max_sugerido`: Nível máximo calculado.
*   `teve_alteracao_analise`: `TRUE` se houve mudança relevante desde a última análise.
*   `data_processamento`: Timestamp da execução.

## 🔄 Fluxo de Alterações

A cada execução, o script:
1.  Busca a **última análise** válida no PostgreSQL.
2.  Gera a **nova análise** com dados frescos do ERP.
3.  Compara linha a linha:
    *   Se `Min` ou `Max` sugerido mudou -> `teve_alteracao = TRUE`
    *   Se `Curva ABC` mudou -> `teve_alteracao = TRUE`
    *   Caso contrário -> `FALSE`
4.  Salva os novos dados com a flag atualizada.
