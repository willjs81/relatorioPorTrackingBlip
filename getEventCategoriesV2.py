import uuid
import requests
import json
import re
from urllib.parse import quote
from openpyxl import Workbook
from datetime import datetime
from zoneinfo import ZoneInfo
import os
from pathlib import Path
from collections import Counter
from dotenv import load_dotenv

load_dotenv()

URL = os.getenv("URL")
KEY_DUDA_PRD = os.getenv("KEY_DUDA_PRD")
KEY_FGTS_PRD = os.getenv("KEY_FGTS_PRD")
KEY_CLT_PRD = os.getenv("KEY_CLT_PRD")
KEY_DUDA_DEV = os.getenv("KEY_DUDA_DEV")


# ------------------------------------------------------------
# NORMALIZA A ACTION DIGITADA PELO USUÁRIO
# ------------------------------------------------------------
def normalizar_action(action_input: str) -> str:
    s = action_input.strip()

    # Remove aspas externas
    if len(s) >= 2 and (
        (s.startswith('"') and s.endswith('"')) or
        (s.startswith("'") and s.endswith("'"))
    ):
        s = s[1:-1].strip()

    # Remove escape \"
    if '\\"' in s:
        s = s.replace('\\"', '"')

    # Tenta carregar JSON
    try:
        parsed = json.loads(s)
        if isinstance(parsed, dict):
            return json.dumps(parsed, separators=(",", ":"), ensure_ascii=False)
        if isinstance(parsed, str):
            parsed2 = json.loads(parsed)
            return json.dumps(parsed2, separators=(",", ":"), ensure_ascii=False)
    except:
        pass

    return s


def router_slug(nome: str) -> str:
    slug = re.sub(r"[^0-9A-Za-z]+", "_", nome.strip()).strip("_").lower()
    return slug or "router"


def escolher_autorizacao():
    print("\nSelecione o router para usar a chave de autorização:")
    print("1 - Duda PRD")
    print("2 - Campanhas FGTS PRD (padrão)")
    print("3 - Campanhas Emp Privado")
    print("4 - Duda DEV")

    opcao = input("Opção (1/2/3/4, ENTER = 2): ").strip()

    if opcao == "1":
        token = KEY_DUDA_PRD
        nome = "Duda PRD"
    elif opcao == "3":
        token = KEY_CLT_PRD
        nome = "Campanhas Emp Privado"
    elif opcao == "4":
        token = KEY_DUDA_DEV
        nome = "Duda DEV"
    else:
        token = KEY_FGTS_PRD
        nome = "Campanhas FGTS PRD"

    if not token:
        print(f"\nChave não encontrada para {nome}. Verifique o .env.\n")
        return None, None, None

    print(f"\nUsando chave do router: {nome}")
    return token, router_slug(nome), nome


def storage_date_br(storage_date_str: str) -> str:
    """Converte storageDate UTC para America/Sao_Paulo mantendo ISO-like."""
    if not storage_date_str:
        return ""

    try:
        iso_str = storage_date_str.replace("Z", "+00:00")
        dt_utc = datetime.fromisoformat(iso_str)
        dt_br = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
        return dt_br.isoformat().replace("+00:00", "Z")
    except Exception:
        return storage_date_str

def normalizar_data(data_input: str) -> str | None:
    s = (data_input or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        pass
    try:
        return datetime.strptime(s, "%d-%m-%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


# ------------------------------------------------------------
# LOOP PRINCIPAL DO PROGRAMA
# ------------------------------------------------------------
while True:
    print("\n===== CONSULTA DE EVENTOS POR ACTION (EVENT CATEGORIES) =====")

    start_date_input = input("Informe a data inicial (DD-MM-AAAA ou YYYY-MM-DD): ").strip()
    end_date_input = input("Informe a data final   (DD-MM-AAAA ou YYYY-MM-DD): ").strip()
    start_date = normalizar_data(start_date_input)
    end_date = normalizar_data(end_date_input)

    if not start_date or not end_date:
        print("\nData invalida. Use DD-MM-AAAA ou YYYY-MM-DD.\n")
        continue

    default_tracking = "md Response api inclusao response tratada"
    tracking_input = input(
        f'Digite o nome do tracking (ENTER para usar o padrão "{default_tracking}"): '
    ).strip()

    tracking = tracking_input if tracking_input else default_tracking
    tracking_encoded = quote(tracking, safe="")

    print("\nCole a ACTION completa:")
    action_raw = input("ACTION: ").strip()

    if not action_raw:
        print("\nNenhuma action informada. Reiniciando...\n")
        continue

    action_normalizada = normalizar_action(action_raw)
    action_encoded = quote(action_normalizada, safe="")

    uri_event = (
        f"/event-track/{tracking_encoded}/"
        f"{action_encoded}"
        f"?startDate={start_date}&endDate={end_date}&$take=500"
    )

    print("\n===== PARÂMETROS DA CONSULTA =====")
    print(f"Data inicial      : {start_date}")
    print(f"Data final        : {end_date}")
    print(f"Tracking          : {tracking}")
    print(f"Action normalizada: {action_normalizada}")
    print(f"URI montada       : {uri_event}")

    authorization, router_dir, router_nome = escolher_autorizacao()
    if not authorization:
        continue

    headers = {
        "Authorization": authorization,
        "Content-Type": "application/json",
    }

    body = {
        "id": str(uuid.uuid4()),
        "to": "postmaster@analytics.msging.net",
        "method": "get",
        "uri": uri_event,
    }

    response = requests.post(URL, json=body, headers=headers)

    print("\n===== STATUS DA REQUISIÇÃO =====")
    print(response.status_code)

    try:
        response.raise_for_status()
    except:
        print("Erro na requisição. Reiniciando...\n")
        continue

    data = response.json()
    resource = data.get("resource", {})
    items = resource.get("items", [])

    print("\n===== RESUMO DA RESPOSTA =====")
    print(f"Total informado pela API : {resource.get('total')}")
    print(f"Itens retornados (items) : {len(items)}")

    if len(items) == 0:
        print("\nNenhum evento encontrado. Reiniciando...\n")
        continue

    # --------------------------------------------------------------------
    # REMOVER DUPLICADOS POR (contactIdentity + action)
    # --------------------------------------------------------------------
    vistos = set()
    itens_unicos = []

    for item in items:
        contact = item.get("contact", {}).get("Identity")
        action = item.get("action")
        chave = (contact, action)

        if chave not in vistos:
            vistos.add(chave)
            itens_unicos.append(item)

    print(f"\nItens únicos após remoção de duplicados: {len(itens_unicos)}")

    # --------------------------------------------------------------------
    # RESUMO ANALÍTICO ANTES DO EXCEL
    # --------------------------------------------------------------------
    contatos = [i.get("contact", {}).get("Identity", "") for i in itens_unicos]
    cpfs = [i.get("extras", {}).get("cpf", "") for i in itens_unicos]

    contatos_unicos = len(set(contatos))
    cpfs_unicos = len(set(cpfs))

    print("\n===== RESUMO ANALÍTICO =====")
    print(f"Total de eventos únicos : {len(itens_unicos)}")
    print(f"Total de contatos únicos: {contatos_unicos}")
    print(f"Total de CPFs únicos    : {cpfs_unicos}")

    # Top 10 contatos com mais ocorrências
    contador_contatos = Counter(contatos)
    top10 = contador_contatos.most_common(10)

    print("\nTOP 10 CONTATOS COM MAIS EVENTOS:")
    for contato, qtd in top10:
        print(f"- {contato}: {qtd} evento(s)")

    # --------------------------------------------------------------------
    # PERGUNTAR SE DEVE GERAR EXCEL
    # --------------------------------------------------------------------
    gerar = input("\nDeseja gerar arquivo Excel com estes resultados (s/n)? ").strip().lower()
    if gerar != "s":
        print("\nExcel não gerado. Encerrando o script.\n")
        break

    # --------------------------------------------------------------------
    # CRIA EXCEL
    # --------------------------------------------------------------------
    wb = Workbook()
    ws = wb.active
    ws.title = "relatorio por action"

    # Metadados do relatório
    ws.append(["Tracking utilizado", tracking])
    ws.append(["Action informada", action_normalizada])
    ws.append([])  # linha em branco para separar cabeçalho

    # Descobre todas as chaves de extras para virar coluna
    extras_keys = sorted({
        key
        for item in itens_unicos
        for key in (item.get("extras") or {}).keys()
    })

    # Cabeçalho fixo + colunas de extras dinamicamente com prefixo "Extras_"
    extras_headers = [f"Extras_{key}" for key in extras_keys]
    ws.append(["storageDate", "storageDateBR", "cpf", "contactIdentity", "category", "action", *extras_headers])

    for item in itens_unicos:
        storage = item.get("storageDate")
        cpf = item.get("extras", {}).get("cpf", "")
        contact = item.get("contact", {}).get("Identity", "")
        category = item.get("category")
        action = item.get("action")
        extras = item.get("extras") or {}
        extras_values = []
        for key in extras_keys:
            value = extras.get(key, "")
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            extras_values.append(value)

        storage_br = storage_date_br(storage)
        ws.append([storage, storage_br, cpf, contact, category, action, *extras_values])

    # Versionamento automático
    today = datetime.now().strftime("%Y%m%d")

    home_dir = Path.home()
    documentos_dir = home_dir / "Documentos"
    if not documentos_dir.exists():
        documentos_dir = home_dir / "Documents"

    relatorio_root = documentos_dir / "relatorio de eventos"
    tipo_subpasta = relatorio_root / "eventCategories" / router_dir
    today_folder = tipo_subpasta / f"relatorio_{today}"
    today_folder.mkdir(parents=True, exist_ok=True)

    base_name = f"eventos_por_action_{router_dir}_{today}"
    file_name = base_name + ".xlsx"
    file_path = today_folder / file_name
    v = 1

    while file_path.exists():
        v += 1
        file_name = f"{base_name}_v{v}.xlsx"
        file_path = today_folder / file_name

    wb.save(file_path)

    print("\n===== ARQUIVO GERADO =====")
    print("Arquivo salvo como:", file_path)
    print("\nEncerrando o script.\n")
    break
