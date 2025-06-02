import pdfkit
from jinja2 import Environment, FileSystemLoader
from datetime import datetime, timedelta

data = []

config = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')

def average_times(times):
    total_time = timedelta()
    count = 0
    for time_str in times:
        if time_str is not None:
            total_time += time_to_timedelta(time_str)
            count += 1

    if count == 0:
        return "00:00:00.000"  # Evitar divisão por zero

    # Calcular a média dividindo pelo número de itens
    avg_time = total_time / count

    # Extrair horas, minutos, segundos e milissegundos do timedelta
    total_seconds = int(avg_time.total_seconds())
    milliseconds = avg_time.microseconds // 1000
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{hours:02}:{minutes:02}:{seconds:02}.{milliseconds:03}"


# Função para converter string de tempo para timedelta (inclui milissegundos)
def time_to_timedelta(time_str):
    try:
        time_obj = datetime.strptime(time_str, "%H:%M:%S.%f")  # Considera milissegundos
    except ValueError:
        time_obj = datetime.strptime(
            time_str, "%H:%M:%S"
        )  # Caso não tenha milissegundos

    return timedelta(
        hours=time_obj.hour,
        minutes=time_obj.minute,
        seconds=time_obj.second,
        milliseconds=time_obj.microsecond // 1000,
    )


# Função para somar uma lista de tempos e retornar no formato hh:mm:ss.mmm
def sum_times(times):
    total_time = timedelta()
    for time_str in times:
        if time_str is not None:  # Ignorar valores nulos
            total_time += time_to_timedelta(time_str)

    # Extrair horas, minutos, segundos e milissegundos do timedelta
    total_seconds = int(total_time.total_seconds())
    milliseconds = total_time.microseconds // 1000
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{hours:02}:{minutes:02}:{seconds:02}.{milliseconds:03}"


# Função para calcular a soma de porcentagens
def sum_percentages(percentages):
    return sum(float(p.strip("%")) for p in percentages)


# Função para calcular a média de porcentagens
def avg_percentages(percentages):
    valid_percentages = [
        float(p.strip("%")) for p in percentages if p != "N/A"
    ]  # Filter out 'N/A'
    if not valid_percentages:  # Handle case where no valid percentages exist
        return "0.00%"
    total = sum(valid_percentages)
    avg = total / len(valid_percentages)
    return f"{avg:.2f}%"


def create_pdf():
    # Lista de meses para o nome
    meses = [
        "Janeiro",
        "Fevereiro",
        "Março",
        "Abril",
        "Maio",
        "Junho",
        "Julho",
        "Agosto",
        "Setembro",
        "Outubro",
        "Novembro",
        "Dezembro",
    ]

    # Corrigir o cálculo do total para garantir precisão
    def safe_int(val):
        try:
            return int(round(float(val)))
        except Exception:
            return 0

    # Debug: imprimir todos os totais individuais antes de somar
    print("Totais individuais por campanha:")
    for item in data:
        print(f"{item.get('nome')}: {item.get('total')}")

    totals = {
        "total": sum(safe_int(item.get("total", 0)) for item in data),
        "total_atend": sum(safe_int(item.get("total_atend", 0)) for item in data),
        "aban": sum(item["aban"] for item in data),
        "aban_percent": str(
            avg_percentages([str(item["aban_percent"]) for item in data])
        ),
        "tma": average_times([item["tma"] for item in data]),
        "tme": average_times([item["tme"] for item in data]),
        "sla": avg_percentages([str(item["sl"]) for item in data]),
        "qtde": sum(item["qtde"] for item in data),
        "slr": avg_percentages([str(item["slr"]) for item in data]),
    }

    print(f"Soma total calculada: {totals['total']}")

    # Caminho para o template HTML
    template_dir = "."
    template_file = "relatorio.html"

    # Configurar Jinja2 para carregar o template
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template(template_file)

    # Pegando o mês para colocar no nome
    hoje = datetime.today()
    primeiro_dia_mes_atual = hoje.replace(day=1)
    ultimo_dia_mes_anterior = primeiro_dia_mes_atual - timedelta(days=1)
    previousMonth = meses[ultimo_dia_mes_anterior.month - 1]

    # Caminho para o arquivo CSS e PDF
    css_file = "relatorio.css"
    output_pdf = f"relatorios\\Relatorio_{previousMonth}.pdf"

    # Data do relatório
    issue_date = datetime.now().strftime("%d/%m/%Y")
    issue_time = datetime.now().strftime("%H:%M")

    # Gerar o HTML com os dados
    html_content = template.render(
        data=data, totals=totals, issue_date=issue_date, issue_time=issue_time
    )

    # Gerar o PDF
    options = {"no-outline": None, "enable-local-file-access": None}

    pdfkit.from_string(html_content, output_pdf, configuration=config, options=options, css=css_file)

    print(f"PDF gerado com sucesso: {output_pdf}")
