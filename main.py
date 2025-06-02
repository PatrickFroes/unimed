import asyncio
from five9 import Five9
from gerar_pdf import data, create_pdf
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

# 1.2 - Remover uso de locale, usar dicionário para meses
meses_pt_to_en = {
    'jan': 'Jan', 'fev': 'Feb', 'mar': 'Mar', 'abr': 'Apr', 'mai': 'May',
    'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug', 'set': 'Sep', 'out': 'Oct',
    'nov': 'Nov', 'dez': 'Dec'
}

# Adicione este dicionário para os dias da semana
dias_pt_to_en = {
    'seg': 'Mon', 'ter': 'Tue', 'qua': 'Wed', 'qui': 'Thu',
    'sex': 'Fri', 'sáb': 'Sat', 'sab': 'Sat', 'dom': 'Sun'
}

all_transformed_data = []

filename = r"credentials.txt"
with open(filename, "rt") as f:
    text = f.readline()
username, password = text.strip().split()

client = Five9(username=username, password=password)
# 1.1 - Certifique-se de que as datas estejam em UTC (Five9 espera UTC)
# Ajuste aqui para garantir que o período seja o correto em Brasília
start_brasilia = datetime(2025, 5, 1, 0, 0, 0)
end_brasilia = datetime(2025, 5, 31, 23, 59, 59)

def brasilia_to_utc_str(dt_brasilia):
    dt_utc = dt_brasilia - timedelta(hours=3)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000")

start = brasilia_to_utc_str(start_brasilia)
# Corrija aqui: não adicione segundos/milissegundos extras, apenas converta normalmente
end = brasilia_to_utc_str(end_brasilia)

def split_periods(start_str, end_str, n=4):
    # 1.1 - Parse as datas como UTC
    start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
    total_seconds = (end_dt - start_dt).total_seconds()
    period_seconds = total_seconds / n
    periods = []
    for i in range(n):
        period_start = start_dt + timedelta(seconds=i * period_seconds)
        period_end = start_dt + timedelta(seconds=(i + 1) * period_seconds - 1)
        if i == n - 1:
            period_end = end_dt
        periods.append((
            period_start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
            period_end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        ))
    return periods

async def run_report_async(client, folder_name, report_name, criteria):
    loop = asyncio.get_event_loop()
    identifier = await loop.run_in_executor(
        None, client.configuration.runReport, folder_name, report_name, criteria
    )
    await asyncio.sleep(12)
    return identifier

async def get_report_result_async(client, identifier):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, client.configuration.getReportResult, identifier
    )
    return result

async def getReturn(period_criteria):
    # 1.3 - Refatorar lógica para considerar ordem cronológica e precisão de hora
    campanhas = [
        "Unimed Anápolis", "Unimed Araguaína", "Unimed Caldas Novas", "Unimed Catalão",
        "Unimed Gurupi", "Unimed Jataí", "Unimed Mineiros", "Unimed Morrinhos",
        "Unimed Regional Sul", "Unimed Rio Verde", "Unimed Vale do Corumbá", "Unimed Cerrado"
    ]
    retorno_dict = {camp: 0 for camp in campanhas}
    criteria = {"time": {"end": period_criteria["end"], "start": period_criteria["start"]}}

    identifier = await run_report_async(client, "MyReports", "Abandonadas", criteria)
    get_results = await get_report_result_async(client, identifier)
    identifier = await run_report_async(client, "MyReports", "Contacted", criteria)
    get_results2 = await get_report_result_async(client, identifier)

    # 1.2 - Corrigir parsing de datas com meses em português
    def parse_five9_date(date_str):
        # Exemplo: 'sex, 2 mai 2025 03:33:38'
        parts = date_str.split()
        if len(parts) < 5:
            raise ValueError("Formato de data inesperado: " + date_str)
        dia_pt = parts[0].replace(',', '').lower()
        dia_en = dias_pt_to_en.get(dia_pt, dia_pt)
        parts[0] = dia_en + ','

        mes_pt = parts[2].lower()
        mes_en = meses_pt_to_en.get(mes_pt, mes_pt)
        parts[2] = mes_en

        date_str_en = " ".join(parts)
        return datetime.strptime(date_str_en, "%a, %d %b %Y %H:%M:%S")

    # 1.3 - Montar listas com precisão de hora/minuto/segundo
    lista_abandonadas = []
    for record in get_results["records"]:
        record_data = record["values"]["data"]
        campanha = record_data[2]
        dt = parse_five9_date(record_data[1])
        info = (record_data[0], dt, campanha)
        lista_abandonadas.append(info)

    lista_contactadas = []
    for record in get_results2["records"]:
        record_data = record["values"]["data"]
        campanha = record_data[2]
        dt = parse_five9_date(record_data[1])
        info = (record_data[0], dt, campanha)
        lista_contactadas.append(info)

    # 1.3 - Para cada abandonada, procurar contato posterior na mesma campanha
    for nome, dt_aband, campanha in lista_abandonadas:
        for nome2, dt_cont, campanha2 in lista_contactadas:
            if (
                nome == nome2
                and campanha == campanha2
                and dt_cont > dt_aband
                and (dt_cont - dt_aband).total_seconds() <= 86400  # até 24h depois
            ):
                retorno_dict[campanha] += 1
                break

    return retorno_dict

async def getRelatorioChamadas(period_criteria, transformed_data_list):
    criteria = {"time": {"end": period_criteria["end"], "start": period_criteria["start"]}}
    identifier = await run_report_async(client, "MyReports", "Relatório Chamadas", criteria)
    get_results = await get_report_result_async(client, identifier)
    for record in get_results["records"]:
        record_data = record["values"]["data"]
        if record_data[3] is None:
            record_data[3] = 0

        # Corrigir o cálculo do total para garantir precisão e evitar -1
        def safe_total(val):
            try:
                if val is None:
                    return 0
                if isinstance(val, str):
                    val = val.replace(',', '.')
                    return int(round(float(val)))
                if isinstance(val, float):
                    return int(round(val))
                return int(val)
            except Exception:
                return 0

        transformed_data = {
            "nome": record_data[0],
            "total_atend": int(record_data[1]) if record_data[1] else 0,
            "total": safe_total(record_data[4]),
            "aban": (
                round(float(record_data[2]), 2) if record_data[2] is not None else 0
            ),
            "aban_percent": (
                float(
                    (
                        Decimal(record_data[2])
                        / Decimal(record_data[4])
                        * Decimal("100")
                    ).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
                )
                if record_data[2] and record_data[1]
                else 0
            ),
            # Adding placeholders for fields from other reports
            "tma": "00:00:00",
            "tme": "00:00:00",
            "sl": "N/A",
            "qtde": "N/A",
            "slr": "N/A",
        }
        transformed_data_list.append(transformed_data)

async def getTmaTme(period_criteria, transformed_data_list):
    criteria = {"time": {"end": period_criteria["end"], "start": period_criteria["start"]}}
    identifier = await run_report_async(client, "MyReports", "Relatório Chamadas (TMA e TME)", criteria)
    get_results_2 = await get_report_result_async(client, identifier)
    for idx, record in enumerate(get_results_2["records"]):
        record_data_2 = record["values"]["data"]
        if idx < len(transformed_data_list):
            transformed_data_list[idx].update(
                {
                    "tma": (
                        record_data_2[3] if record_data_2[3] is not None else "00:00:00"
                    ),
                    "tme": (
                        record_data_2[4] if record_data_2[4] is not None else "00:00:00"
                    ),
                }
            )

async def getRelatorioSLA(period_criteria, transformed_data_list):
    criteria = {"time": {"end": period_criteria["end"], "start": period_criteria["start"]}}
    identifier = await run_report_async(client, "MyReports", "Relatório com o SLA", criteria)
    get_results_3 = await get_report_result_async(client, identifier)
    for idx, record in enumerate(get_results_3["records"]):
        record_data_3 = record["values"]["data"]
        if idx < len(transformed_data_list):
            transformed_data_list[idx].update(
                {
                    "sl": record_data_3[2] if record_data_3[2] else "N/A",
                }
            )

def merge_data(list_of_lists):
    merged = {}
    for data_list in list_of_lists:
        for item in data_list:
            nome = item["nome"]
            if nome not in merged:
                merged[nome] = item.copy()
            else:
                merged[nome]["total"] += item["total"]
                merged[nome]["total_atend"] += item["total_atend"]
                merged[nome]["aban"] += item["aban"]
                merged[nome]["qtde"] = (
                    merged[nome].get("qtde", 0) + (item.get("qtde", 0) if isinstance(item.get("qtde", 0), (int, float)) else 0)
                )
                # Para campos percentuais e tempos, sobrescreva ou trate depois
    # Após somar, re-calcule percentuais e tempos médios
    merged_list = list(merged.values())
    return merged_list

async def main():
    periods = split_periods(start, end, 4)
    all_data_lists = []
    all_retorno_dicts = []
    for period_start, period_end in periods:
        period_criteria = {"start": period_start, "end": period_end}
        transformed_data_list = []
        await asyncio.gather(
            getRelatorioChamadas(period_criteria, transformed_data_list),
            getTmaTme(period_criteria, transformed_data_list),
            getRelatorioSLA(period_criteria, transformed_data_list),
        )
        retornos = await getReturn(period_criteria)
        for item in transformed_data_list:
            nome_campanha = item["nome"]
            if nome_campanha in retornos:
                item["qtde"] = float(retornos[nome_campanha])
            else:
                item["qtde"] = 0.0
        all_data_lists.append(transformed_data_list)
        all_retorno_dicts.append(retornos)

    merged_data = merge_data(all_data_lists)
    # 1.4 e 1.5 - Corrigir cálculo de SLA e SLR
    for item in merged_data:
        total_atend = float(item.get("total_atend", 0))
        total = float(item.get("total", 0))
        qtde = float(item.get("qtde", 0))
        # SLA: ((total_atend) * 100) / total
        if total > 0:
            sla = (total_atend * 100) / total
        else:
            sla = 0.0
        item["sl"] = f"{sla:.2f}%"
        # SLR: ((total_atend + retorno) * sla) / total_atend
        if total_atend > 0:
            slr = ((total_atend + qtde) * sla) / total_atend
        else:
            slr = 0.0
        if slr > 100.0:
            slr = 100.0
        item["slr"] = f"{slr:.2f}"
    data.clear()
    data.extend(merged_data)
    create_pdf()

asyncio.run(main())
