FROM python:3.11-slim

# Metadados
LABEL maintainer="Sistema Analise Estoque"
LABEL description="Serviço de Análise Semanal de Estoque (FIFO)"

# Argumentos de build
ARG DEBIAN_FRONTEND=noninteractive

# Definir diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema
# gcc, g++: para compilar drivers se necessario
# unixodbc-dev, freetds-dev, tdsodbc: para conectar SQL Server
# postgresql-client: utilitarios postgres
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    unixodbc \
    unixodbc-dev \
    freetds-dev \
    freetds-bin \
    tdsodbc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Configurar Driver FreeTDS
RUN echo "[FreeTDS]\n\
    Description = FreeTDS Driver\n\
    Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
    Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini

# Copiar e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação
COPY main.py .

# Criar diretório de dados/cache se necessario
RUN mkdir -p /app/data

# Definir variaveis de ambiente padrao (podem ser sobrescritas)
ENV INTERVALO_DIAS=7
ENV PYTHONUNBUFFERED=1

# Expor a porta da API
EXPOSE 8000

# Comando de execução
CMD ["python", "-m", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
