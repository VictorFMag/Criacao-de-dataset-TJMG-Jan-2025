import requests
import json
import time
from datetime import datetime, timedelta
import os

# ------------------------------
# CONFIGURAÇÕES
# ------------------------------
BASE_URL = "https://comunicaapi.pje.jus.br/api/v1/comunicacao"
SIGLA_TRIBUNAL = "TJMG"
DATA_INICIO = datetime(2025, 1, 1)
DATA_FIM = datetime(2025, 1, 31)
ARQUIVO_SAIDA = "casos_tjmg_jan2025.json"
ARQUIVO_BACKUP = "casos_tjmg_backup.json"
ARQUIVO_ERROS = "erros.log"

# ------------------------------
# FUNÇÕES AUXILIARES
# ------------------------------

def registrar_erro(msg):
    """Salva mensagens de erro em um log."""
    with open(ARQUIVO_ERROS, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] {msg}\n")

def salvar_backup(dados):
    """Salva backup do progresso em JSON."""
    with open(ARQUIVO_BACKUP, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=4)
    print(f"[INFO] Backup salvo com {len(dados)} itens")

def coletar_casos_dia(data_inicio, data_fim, max_retries=3):
    """Coleta todos os casos de um dia com paginação e tratamento de erros."""
    todos_casos = []
    pagina = 1
    falhas_consecutivas = 0

    while True:
        params = {
            "siglaTribunal": SIGLA_TRIBUNAL,
            "dataDisponibilizacaoInicio": data_inicio,
            "dataDisponibilizacaoFim": data_fim,
            "pagina": pagina,
            "itensPorPagina": 100
        }

        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)

            if resp.status_code == 200:
                dados = resp.json()
                itens = dados.get("items", [])
                todos_casos.extend(itens)
                print(f"[OK] {data_inicio} - Página {pagina} ({len(itens)} itens)")

                # Se retornou menos de 100 → acabou
                if len(itens) < 100:
                    break

                pagina += 1
                falhas_consecutivas = 0  # reset
                continue

            # Se deu erro, tenta novamente com backoff
            else:
                falhas_consecutivas += 1
                registrar_erro(f"Erro {resp.status_code} na página {pagina} ({data_inicio})")

                if falhas_consecutivas >= max_retries:
                    print(f"[AVISO] {max_retries} falhas seguidas — pulando o dia {data_inicio}")
                    break

                print(f"[WARN] Erro {resp.status_code}, aguardando 60s...")
                time.sleep(60)
                continue

        except requests.RequestException as e:
            falhas_consecutivas += 1
            registrar_erro(f"Exceção: {e} (página {pagina}, dia {data_inicio})")

            if falhas_consecutivas >= max_retries:
                print(f"[ERRO] {max_retries} falhas consecutivas — pulando o dia {data_inicio}")
                break

            print(f"[WARN] Erro de conexão — aguardando 60s antes de tentar novamente...")
            time.sleep(60)

    return todos_casos

# ------------------------------
# ROTINA PRINCIPAL
# ------------------------------

def main():
    todos_casos = []

    # Se já existe um backup, recupera o progresso
    if os.path.exists(ARQUIVO_BACKUP):
        try:
            with open(ARQUIVO_BACKUP, "r", encoding="utf-8") as f:
                todos_casos = json.load(f)
            print(f"[INFO] Backup carregado com {len(todos_casos)} casos anteriores")
        except Exception as e:
            registrar_erro(f"Erro ao carregar backup: {e}")

    data_atual = DATA_INICIO
    while data_atual <= DATA_FIM:
        dia_str = data_atual.strftime("%Y-%m-%d")

        print(f"\n=== Coletando casos de {dia_str} ===")
        casos_dia = coletar_casos_dia(dia_str, dia_str)

        if casos_dia:
            todos_casos.extend(casos_dia)
            print(f"[INFO] {len(casos_dia)} casos adicionados ({len(todos_casos)} total)")
            salvar_backup(todos_casos)

        data_atual += timedelta(days=1)

    # Salva resultado final
    with open(ARQUIVO_SAIDA, "w", encoding="utf-8") as f:
        json.dump(todos_casos, f, ensure_ascii=False, indent=4)

    print(f"\n✅ Coleta concluída! Total: {len(todos_casos)} casos salvos em '{ARQUIVO_SAIDA}'.")

# ------------------------------
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INTERRUPÇÃO] Execução cancelada manualmente. Salvando progresso...")
        # salva tudo o que tiver até agora
        salvar_backup([])
    except Exception as e:
        registrar_erro(f"Erro fatal: {e}")
        print(f"[ERRO FATAL] {e}")
