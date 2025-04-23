import asyncio
from five9 import Five9
from gerar_pdf import data, create_pdf
from datetime import datetime
import locale
from decimal import Decimal, ROUND_HALF_UP

locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")

# Initialize an empty list to store all transformed data records
all_transformed_data = []

# Reading the username and password
filename = r"credentials.txt"
with open(filename, "rt") as f:
    text = f.readline()
username, password = text.strip().split()

# Five9 parameters
client = Five9(username=username, password=password)
start = "2025-03-01T00:00:00.000"
end = "2025-03-31T23:59:59.000"

async def run_report_async(client, folder_name, report_name, criteria):
    loop = asyncio.get_event_loop()
    identifier = await loop.run_in_executor(
        None, client.configuration.runReport, folder_name, report_name, criteria
    )
    await asyncio.sleep(12) 
     # Simulate delay for report generation
    return identifier

async def get_report_result_async(client, identifier):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, client.configuration.getReportResult, identifier
    )
    return result

async def getReturn():
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

    criteria = {"time": {"end": end, "start": start}}

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
    print(get_results2)

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

async def getRelatorioChamadas():
    criteria = {"time": {"end": end, "start": start}}
    identifier = await run_report_async(client, "MyReports", "Relatório Chamadas", criteria)
    get_results = await get_report_result_async(client, identifier)

    for record in get_results["records"]:
        record_data = record["values"]["data"]
        if record_data[3] is None:
            record_data[3] = 0
        # Initialize transformed data for each record
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
        all_transformed_data.append(transformed_data)

async def getTmaTme():
    criteria = {"time": {"end": end, "start": start}}
    identifier = await run_report_async(client, "MyReports", "Relatório Chamadas (TMA e TME)", criteria)
    get_results_2 = await get_report_result_async(client, identifier)

    for idx, record in enumerate(get_results_2["records"]):
        record_data_2 = record["values"]["data"]
        # Update the respective record in the list with TMA and TME
        if idx < len(all_transformed_data):
            all_transformed_data[idx].update(
                {
                    "tma": (
                        record_data_2[3] if record_data_2[3] is not None else "00:00:00"
                    ),
                    "tme": (
                        record_data_2[4] if record_data_2[4] is not None else "00:00:00"
                    ),
                }
            )

async def getRelatorioSLA():
    criteria = {"time": {"end": end, "start": start}}
    identifier = await run_report_async(client, "MyReports", "Relatório com o SLA", criteria)
    get_results_3 = await get_report_result_async(client, identifier)

    for idx, record in enumerate(get_results_3["records"]):
        record_data_3 = record["values"]["data"]
        # Update the respective record in the list with atend_esp and sl
        if idx < len(all_transformed_data):
            all_transformed_data[idx].update(
                {
                    "sl": record_data_3[2] if record_data_3[2] else "N/A",
                }
            )

async def main():
    await asyncio.gather(
        getRelatorioChamadas(),
        getTmaTme(),
        getRelatorioSLA(),
    )
    retornos = await getReturn()

    # Integra o valor de "qtde" de retornos em all_transformed_data
    for item in all_transformed_data:
        nome_campanha = item["nome"]
        if nome_campanha in retornos:
            item["qtde"] = float(retornos[nome_campanha])
        else:
            item["qtde"] = 0.0

    # Calcula o SLR (retornos/aban) agora utilizando o campo "qtde"
    for item in all_transformed_data:
        aban = float(item.get("aban", 0))
        qtde = float(item.get("qtde", 0))
        total_atend = float(item.get("total_atend", 0))
        total = float(item.get("total", 0))
        sla = str(item.get("sl", 0))
        sla = float(sla.strip("%"))

        if sla > 0.0:
            slr = (qtde / total_atend) + sla if aban != 0 else 0.0
        else:
            slr = sla

        if slr >= 100.0:
            slr = 100.00

        item["slr"] = f"{slr:.2f}"

    # Append all data to the PDF generation list and create the PDF
    data.extend(all_transformed_data)
    create_pdf()

# Run the main function
asyncio.run(main())
