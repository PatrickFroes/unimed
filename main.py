import asyncio
from five9 import Five9
from gerar_pdf import data, create_pdf
from datetime import datetime, timedelta
import locale
from decimal import Decimal, ROUND_HALF_UP

locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")

all_transformed_data = []

filename = r"credentials.txt"
with open(filename, "rt") as f:
    text = f.readline()
username, password = text.strip().split()

client = Five9(username=username, password=password)
start = "2025-03-01T00:00:00.000"
end = "2025-03-31T23:59:59.000"

def split_periods(start_str, end_str, n=4):
    start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%S.%f")
    end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H:%M:%S.%f")
    total_seconds = (end_dt - start_dt).total_seconds()
    period_seconds = total_seconds / n
    periods = []
    for i in range(n):
        period_start = start_dt + timedelta(seconds=i * period_seconds)
        period_end = start_dt + timedelta(seconds=(i + 1) * period_seconds - 1)
        # Ajuste para o último período terminar exatamente no end_dt
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
    anapolis = 0
    cerrado = 0
    araguaina = 0
    caldas = 0
    catalao = 0
    gurupi = 0
    jatai = 0
    mineiros = 0
    morrinhos = 0
    regional = 0
    rioVerde = 0
    valeCorumba = 0

    criteria = {"time": {"end": period_criteria["end"], "start": period_criteria["start"]}}

    identifier = await run_report_async(client, "MyReports", "Abandonadas", criteria)
    get_results = await get_report_result_async(client, identifier)

    formato = "%a, %d %b %Y %H:%M:%S"
    lista_abandonadas = []
    for record in get_results["records"]:
        record_data = record["values"]["data"]
        campanha = record_data[2]
        dt = datetime.strptime(record_data[1], formato)
        info = (record_data[0], dt.strftime("%Y-%m-%d %H:%M:%S"), campanha)
        lista_abandonadas.append(info)

    identifier = await run_report_async(client, "MyReports", "Contacted", criteria)
    get_results2 = await get_report_result_async(client, identifier)
    # print(get_results2)

    lista_contactadas = []
    for record in get_results2["records"]:
        record_data = record["values"]["data"]
        campanha = record_data[2]
        dt = datetime.strptime(record_data[1], formato)
        info = (record_data[0], dt.strftime("%Y-%m-%d %H:%M:%S"), campanha)
        lista_contactadas.append(info)

    for i in lista_contactadas:
        print(i)

    # Adjust datetime format for comparison
    lista_abandonadas = [
        (record[0], datetime.strptime(record[1], "%Y-%m-%d %H:%M:%S").replace(minute=0, second=0, microsecond=0), record[2])
        for record in lista_abandonadas
    ]
    lista_contactadas = [
        (record[0], datetime.strptime(record[1], "%Y-%m-%d %H:%M:%S").replace(minute=0, second=0, microsecond=0), record[2])
        for record in lista_contactadas
    ]

    # Calculate intersection based on adjusted datetime
    intersection = [i for i in lista_abandonadas if i in lista_contactadas]

    try:
        for i in intersection:
            match i[2]:
                case "Unimed Anápolis":
                    anapolis += 1
                case "Unimed Araguaína":
                    araguaina += 1
                case "Unimed Caldas Novas":
                    caldas += 1
                case "Unimed Catalão":
                    catalao += 1
                case "Unimed Gurupi":
                    gurupi += 1
                case "Unimed Jataí":
                    jatai += 1
                case "Unimed Mineiros":
                    mineiros += 1
                case "Unimed Morrinhos":
                    morrinhos += 1
                case "Unimed Regional Sul":
                    regional += 1
                case "Unimed Rio Verde":
                    rioVerde += 1
                case "Unimed Vale do Corumbá":
                    valeCorumba += 1
                case "Unimed Cerrado":
                    cerrado += 1
                case _:
                    print(f"Campanha não encontrada: {i[2]}")
    except Exception as e:
        print(f"Erro ao processar a lista de abandonadas: {e}")

    return {
        "Unimed Anápolis": anapolis,
        "Unimed Araguaína": araguaina,
        "Unimed Caldas Novas": caldas,
        "Unimed Catalão": catalao,
        "Unimed Gurupi": gurupi,
        "Unimed Jataí": jatai,
        "Unimed Mineiros": mineiros,
        "Unimed Morrinhos": morrinhos,
        "Unimed Regional Sul": regional,
        "Unimed Rio Verde": rioVerde,
        "Unimed Vale do Corumbá": valeCorumba,
        "Unimed Cerrado": cerrado,
    }

async def getRelatorioChamadas(period_criteria, transformed_data_list):
    criteria = {"time": {"end": period_criteria["end"], "start": period_criteria["start"]}}
    identifier = await run_report_async(client, "MyReports", "Relatório Chamadas", criteria)
    get_results = await get_report_result_async(client, identifier)
    for record in get_results["records"]:
        record_data = record["values"]["data"]
        if record_data[3] is None:
            record_data[3] = 0
        transformed_data = {
            "nome": record_data[0],
            "total_atend": int(record_data[1]) if record_data[1] else 0,
            "total": int(record_data[4]) if record_data[4] else 0,
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
        # Integra o valor de "qtde" de retornos em transformed_data_list
        for item in transformed_data_list:
            nome_campanha = item["nome"]
            if nome_campanha in retornos:
                item["qtde"] = float(retornos[nome_campanha])
            else:
                item["qtde"] = 0.0
        all_data_lists.append(transformed_data_list)
        all_retorno_dicts.append(retornos)

    # Unir todos os dados dos períodos
    merged_data = merge_data(all_data_lists)
    # Recalcular campos percentuais e tempos
    for item in merged_data:
        aban = float(item.get("aban", 0))
        qtde = float(item.get("qtde", 0))
        total_atend = float(item.get("total_atend", 0))
        total = float(item.get("total", 0))
        sla = str(item.get("sl", 0))
        try:
            sla_float = float(sla.strip("%"))
        except Exception:
            sla_float = 0.0
        if sla_float > 0.0:
            slr = (qtde / total_atend) + sla_float if aban != 0 else 0.0
        else:
            slr = sla_float
        if slr >= 100.0:
            slr = 100.00
        item["slr"] = f"{slr:.2f}"
    data.extend(merged_data)
    create_pdf()

asyncio.run(main())
