def run_dq_functions():

    print("Iniciando Data Quality")

    import boto3
    import time
    from src.data_quality import dq_queries
    import smtplib
    from email.mime.text import MIMEText
    from airflow.models import Variable
    from dotenv import load_dotenv
    import os

    load_dotenv()

    ATHENA_DB = "movies_silver"
    ATHENA_OUTPUT = "s3://movies-raw-gustavo-portfolio/athena-results/"

    athena = boto3.client("athena")

    # ===========================
    # ENVIO DE EMAIL
    # ===========================

    def send_email_alert(message):

        sender = Variable.get("SENDER_EMAIL")
        receiver = Variable.get("RECEIVER_EMAIL")
        password = Variable.get("EMAIL_PASSWORD")

        msg = MIMEText(message)
        msg["Subject"] = "ALERTA DATA QUALITY"
        msg["From"] = sender
        msg["To"] = receiver

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.send_message(msg)

    # ===========================
    # EXECUTAR QUERY ATHENA
    # ===========================

    def run_query(query):

        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": ATHENA_DB},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )

        execution_id = response["QueryExecutionId"]

        while True:
            result = athena.get_query_execution(QueryExecutionId=execution_id)
            status = result["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

            time.sleep(2)

        result = athena.get_query_results(QueryExecutionId=execution_id)

        value = result["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]

        return int(value)

    # ===========================
    # DATA QUALITY
    # ===========================

    def run_data_quality_checks():

        print("Iniciando queries")

        checks = {
            "rows_last_batch": dq_queries.ROWS_LAST_BATCH,
            "duplicate_ids": dq_queries.DUPLICATE_IDS,
            "null_release_date": dq_queries.NULL_RELEASE_DATE,
            "invalid_vote_range": dq_queries.INVALID_VOTE_RANGE,
            "negative_runtime": dq_queries.NEGATIVE_RUNTIME,
            "revenue_lt_budget": dq_queries.REVENUE_LT_BUDGET,
        }

        results = {}
        alerts = []
        critical_errors = []

        for check_name, query in checks.items():

            result = run_query(query)

            results[check_name] = result

            print(f"{check_name}: {result}")

            if result > 0:

                alert_message = f"{check_name} detected: {result}"

                # erros críticos
                if check_name in ["duplicate_ids", "negative_runtime", "revenue_lt_budget"]:
                    critical_errors.append(alert_message)
                    alerts.append(alert_message)

        # ===========================
        # ENVIAR EMAIL
        # ===========================

        if alerts:

            message = "DATA QUALITY ALERTS\n\n"

            message += "Issues detected:\n"
            message += "\n".join(alerts)

            send_email_alert(message)

        # ===========================
        # QUEBRAR PIPELINE SE CRÍTICO
        # ===========================

        # if critical_errors:
        #     raise ValueError("Critical Data Quality Errors:\n" + "\n".join(critical_errors))

        # return results

    run_data_quality_checks()


if __name__ == "__main__":
    run_dq_functions()