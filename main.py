import asyncio
from five9 import Five9
from gerar_pdf import data, create_pdf
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

meses_pt_to_en = {
    'jan': 'Jan', 'fev': 'Feb', 'mar': 'Mar', 'abr': 'Apr', 'mai': 'May',
    'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug', 'set': 'Sep', 'out': 'Oct',
    'nov': 'Nov', 'dez': 'Dec'
}
dias_pt_to_en = {
    'seg': 'Mon', 'ter': 'Tue', 'qua': 'Wed', 'qui': 'Thu',
    'sex': 'Fri', 'sáb': 'Sat', 'sab': 'Sat', 'dom': 'Sun'
}

filename = r"credentials.txt"
with open(filename, "rt") as f:
    text = f.readline()
username, password = text.strip().split()
client = Five9(username=username, password=password)

# Defina aqui a data de inicio e fim do período que você deseja consultar
# As datas estão no horário de Brasília (UTC-3)
# As datas devem ser definidas no formato Ano-Mês-Dia Hora:Minuto:Segundo.Milissegundo
start_brasilia = datetime(2025, 4, 1, 0, 0, 0, 0)
end_brasilia = datetime(2025, 4, 30, 23, 59, 59, 999000)

def brasilia_to_utc3_str(dt_brasilia):
    return dt_brasilia.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

start = brasilia_to_utc3_str(start_brasilia)
end = brasilia_to_utc3_str(end_brasilia)

def split_periods(start_str, end_str, n=4):
    def strip_offset(dt_str):
        if dt_str.endswith("-03:00"):
            return dt_str[:-6]
        return dt_str
    start_dt = datetime.strptime(strip_offset(start_str), "%Y-%m-%dT%H:%M:%S.%f")
    end_dt = datetime.strptime(strip_offset(end_str), "%Y-%m-%dT%H:%M:%S.%f")
    total_ms = int((end_dt - start_dt).total_seconds() * 1000)
    period_ms = total_ms // n
    periods = []
    for i in range(n):
        if i == 0:
            period_start = start_dt
        else:
            prev_end = datetime.strptime(strip_offset(periods[-1][1]), "%Y-%m-%dT%H:%M:%S.%f")
            period_start = prev_end + timedelta(milliseconds=1)
        if i < n - 1:
            period_end = period_start + timedelta(milliseconds=period_ms - 1)
            if period_end >= end_dt:
                period_end = end_dt - timedelta(milliseconds=(n - i - 1))
        else:
            period_end = end_dt
        periods.append((
            period_start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "-03:00",
            period_end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "-03:00"
        ))
    for i in range(1, len(periods)):
        prev_start, prev_end = periods[i-1]
        curr_start, curr_end = periods[i]
        prev_end_dt = datetime.strptime(strip_offset(curr_start), "%Y-%m-%dT%H:%M:%S.%f") - timedelta(milliseconds=1)
        periods[i-1] = (prev_start, prev_end_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "-03:00")
    last_start, last_end = periods[-1]
    periods[-1] = (last_start, end_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "-03:00")
    return periods

def brasilia_to_iso8601(dt_brasilia):
    return dt_brasilia.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "-03:00"

start = brasilia_to_iso8601(start_brasilia)
end = brasilia_to_iso8601(end_brasilia)

async def run_report_async(client, folder_name, report_name, criteria):
    loop = asyncio.get_event_loop()
    identifier = await loop.run_in_executor(None, client.configuration.runReport, folder_name, report_name, criteria)
    await asyncio.sleep(25)
    return identifier

async def get_report_result_async(client, identifier):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, client.configuration.getReportResult, identifier)
    return result

def parse_five9_date(date_str):
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

async def getReturn(period_criteria):
    campanhas = [
        "Unimed Anápolis", "Unimed Araguaína", "Unimed Caldas Novas", "Unimed Catalão",
        "Unimed Gurupi", "Unimed Jataí", "Unimed Mineiros", "Unimed Morrinhos",
        "Unimed Regional Sul", "Unimed Rio Verde", "Unimed Vale do Corumbá", "Unimed Cerrado"
    ]
    retorno_dict = {camp: 0 for camp in campanhas}
    criteria = {
        "time": {
            "start": period_criteria["start"],
            "end": period_criteria["end"]
        }
    }

    task_aband = asyncio.create_task(run_report_async(client, "MyReports", "Abandonadas", criteria))
    task_contact = asyncio.create_task(run_report_async(client, "MyReports", "Contacted", criteria))
    identifier_aband = await task_aband
    identifier_contact = await task_contact

    task_result_aband = asyncio.create_task(get_report_result_async(client, identifier_aband))
    task_result_contact = asyncio.create_task(get_report_result_async(client, identifier_contact))
    get_results = await task_result_aband
    get_results2 = await task_result_contact

    lista_abandonadas = []
    for record in get_results["records"]:
        record_data = record["values"]["data"]
        campanha = (record_data[2] or "").strip().lower()
        dt = parse_five9_date(record_data[1])
        nome = (record_data[0] or "").strip().lower()
        lista_abandonadas.append((nome, dt, campanha))

    lista_contactadas = []
    for record in get_results2["records"]:
        record_data = record["values"]["data"]
        campanha = (record_data[2] or "").strip().lower()
        dt = parse_five9_date(record_data[1])
        nome = (record_data[0] or "").strip().lower()
        lista_contactadas.append((nome, dt, campanha))

    from collections import defaultdict, deque
    aband_dict = defaultdict(list)
    contact_dict = defaultdict(list)
    for nome, dt, campanha in lista_abandonadas:
        aband_dict[(nome, campanha)].append(dt)
    for nome, dt, campanha in lista_contactadas:
        contact_dict[(nome, campanha)].append(dt)
    for key in aband_dict:
        aband_dict[key].sort()
    for key in contact_dict:
        contact_dict[key].sort()
        contact_dict[key] = deque(contact_dict[key])
    for (nome, campanha), aband_list in aband_dict.items():
        contacts = contact_dict.get((nome, campanha), deque())
        for dt_aband in aband_list:
            idx_to_remove = None
            for idx, dt_cont in enumerate(contacts):
                if dt_cont > dt_aband and (dt_cont - dt_aband).total_seconds() <= 86400:
                    idx_to_remove = idx
                    break
            if idx_to_remove is not None:
                for camp_key in retorno_dict:
                    if campanha == camp_key.strip().lower():
                        retorno_dict[camp_key] += 1
                        break
                del contacts[idx_to_remove]
    return retorno_dict

async def getRelatorioChamadas(period_criteria, transformed_data_list):
    criteria = {
        "time": {
            "start": period_criteria["start"],
            "end": period_criteria["end"]
        }
    }
    identifier = await run_report_async(client, "MyReports", "Relatório Chamadas", criteria)
    get_results = await get_report_result_async(client, identifier)

    def safe_total(val):
        try:
            if val is None:
                return 0
            if isinstance(val, str):
                val = val.replace(',', '.')
                return int(round(float(val)))
            return int(val)
        except Exception:
            return 0

    def safe_int(val):
        try:
            return int(round(float(val)))
        except Exception:
            return 0

    for record in get_results["records"]:
        record_data = record["values"]["data"]
        nome_campanha = (record_data[0] or "").strip()
        total_atend = safe_int(record_data[1])
        aban = safe_int(record_data[2])
        total_raw = safe_total(record_data[4])
        total = total_atend + aban if not total_raw or total_raw < (total_atend + aban) else total_raw
        if total < total_atend:
            total = total_atend
        if total < aban:
            total = aban
        if total > 0:
            aban_percent = float((Decimal(aban) / Decimal(total) * Decimal("100")).quantize(Decimal(".01"), rounding=ROUND_HALF_UP))
        else:
            aban_percent = 0
        transformed_data = {
            "nome": nome_campanha,
            "total_atend": total_atend,
            "total": total,
            "aban": aban,
            "aban_percent": aban_percent,
            "tma": "00:00:00",
            "tme": "00:00:00",
            "sl": "N/A",
            "qtde": 0.0,
            "slr": "N/A",
        }
        transformed_data_list.append(transformed_data)

async def getTmaTme(period_criteria, transformed_data_list):
    criteria = {
        "time": {
            "start": period_criteria["start"],
            "end": period_criteria["end"]
        }
    }
    identifier = await run_report_async(client, "MyReports", "Relatório Chamadas (TMA e TME)", criteria)
    get_results = await get_report_result_async(client, identifier)
    for idx, record in enumerate(get_results["records"]):
        record_data = record["values"]["data"]
        if idx < len(transformed_data_list):
            transformed_data_list[idx]["tma"] = record_data[3] or "00:00:00"
            transformed_data_list[idx]["tme"] = record_data[4] or "00:00:00"

async def getRelatorioSLA(period_criteria, transformed_data_list):
    criteria = {
        "time": {
            "start": period_criteria["start"],
            "end": period_criteria["end"]
        }
    }
    identifier = await run_report_async(client, "MyReports", "Relatório com o SLA", criteria)
    get_results = await get_report_result_async(client, identifier)
    for idx, record in enumerate(get_results["records"]):
        record_data = record["values"]["data"]
        if idx < len(transformed_data_list):
            transformed_data_list[idx]["sl"] = record_data[2] or "N/A"

def merge_data(list_of_lists):
    merged = {}
    unique_keys = set()
    for data_list in list_of_lists:
        for item in data_list:
            key = (
                (item["nome"] or "").strip().lower(),
                int(item.get("total_atend", 0)),
                int(item.get("aban", 0)),
                int(item.get("total", 0))
            )
            if key not in unique_keys:
                unique_keys.add(key)
                nome = key[0]
                if nome not in merged:
                    merged[nome] = item.copy()
                else:
                    merged[nome]["total"] += item["total"]
                    merged[nome]["total_atend"] += item["total_atend"]
                    merged[nome]["aban"] += item["aban"]
                    merged[nome]["qtde"] += item.get("qtde", 0.0)
    merged_list = []
    for nome, item in merged.items():
        item["nome"] = item["nome"].strip()
        merged_list.append(item)
    return merged_list

async def main():
    period_criteria = {"start": start, "end": end}
    transformed_data_list = []

    await asyncio.gather(
        getRelatorioChamadas(period_criteria, transformed_data_list),
        getTmaTme(period_criteria, transformed_data_list),
        getRelatorioSLA(period_criteria, transformed_data_list),
    )
    retornos = await getReturn(period_criteria)
    for item in transformed_data_list:
        retorno = float(retornos.get(item["nome"], 0.0))
        item["qtde"] = retorno
        total_atend_original = float(item.get("total_atend", 0))
        total_atend_com_retorno = total_atend_original + retorno
        total = float(item.get("total", 0))
        aban = float(item.get("aban", 0))
        aban_corrigido = aban - retorno
        if aban_corrigido < 0:
            aban_corrigido = 0
        item["aban"] = aban_corrigido
        # Porcentagem de abandonadas após retorno
        if total > 0:
            aban_percent = float((Decimal(aban_corrigido) / Decimal(total) * Decimal("100")).quantize(Decimal(".01"), rounding=ROUND_HALF_UP))
        else:
            aban_percent = 0
        item["aban_percent"] = aban_percent
        # SLA: ((total_atend + retorno) * 100) / total
        sla = (total_atend_com_retorno * 100 / total) if total else 0
        if sla > 100.0:
            sla = 100.0
        item["total_atend"] = total_atend_com_retorno
        item["sl"] = f"{sla:.2f}%"
        # SLR: ((total_atend + retorno) * sla) / total_atend_original
        if retorno > 0 and total_atend_original > 0:
            slr = ((total_atend_com_retorno) * sla) / total_atend_original
            if slr > 100.0:
                slr = 100.0
            item["slr"] = f"{slr:.2f}%"
        else:
            item["slr"] = "N/A"

    data.clear()
    data.extend(transformed_data_list)
    create_pdf()

if __name__ == "__main__":
    asyncio.run(main())
