"""Menu simples para executar os scripts de relatórios existentes.

Mantém os arquivos originais inalterados e permite escolher qual
script executar em uma única sessão.
"""

import subprocess
import sys
from pathlib import Path


SCRIPTS = {
    "1": ("Relatório de inconsistências (getEventDeatilsV3)", "getEventDeatilsV3.py"),
    "2": ("Eventos por action (getEventCategoriesV2)", "getEventCategoriesV2.py"),
    "3": ("Inconsistencias + actions (consolidado)", "getEventDeatilsV3_com_actions.py"),
    "4": ("Analitico consolidado (bruto da API)", "getEventDeatilsV3_analitico.py"),
}


def run_script(script_path: Path) -> None:
    """Executa o script informado usando o mesmo interpretador Python."""
    if not script_path.exists():
        print(f"Arquivo não encontrado: {script_path}")
        return

    print(f"\n=== Executando {script_path.name} ===\n")
    try:
        subprocess.run([sys.executable, str(script_path)], check=True)
    except subprocess.CalledProcessError as err:
        print(f"Erro ao executar {script_path.name}: {err}")
    finally:
        print(f"\n=== Retornando ao menu principal ===\n")


def main() -> None:
    base_dir = Path(__file__).resolve().parent

    while True:
        print("\n===== MENU DE RELATÓRIOS =====")
        for opcao, (descricao, _) in sorted(SCRIPTS.items()):
            print(f"{opcao} - {descricao}")
        print("0 - Sair")

        escolha = input("Escolha uma opção: ").strip()

        if escolha == "0":
            print("Saindo do menu. Até logo!")
            break

        info = SCRIPTS.get(escolha)
        if not info:
            print("Opção inválida. Tente novamente.")
            continue

        descricao, arquivo = info
        script_path = base_dir / arquivo
        run_script(script_path)


if __name__ == "__main__":
    main()
