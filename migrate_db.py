from main import criar_tabela_postgres
import sys
import os

# Adiciona o diretorio atual ao path para garantir que imports funcionem
sys.path.append(os.getcwd())

if __name__ == "__main__":
    print("Iniciando migração do banco de dados...")
    try:
        criar_tabela_postgres()
        print("Migração concluída com sucesso!")
    except Exception as e:
        print(f"Erro ao executar migração: {e}")
        exit(1)
