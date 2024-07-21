# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 mainApp.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    ArrayType
)
from project_function import decyzja_kredytowa
from fpdf import FPDF
import unicodedata
import csv

if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # json message schema
    json_schema = StructType(
        [
            StructField("ID", StringType()),
            StructField("Outstanding_Debt", FloatType()),
            StructField("Credit_Mix", StringType()),
            StructField("Num_Credit_Card", IntegerType()),
            StructField("Interest_Rate", IntegerType()),
        ]
    )
    
    # topic subscription
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test")
        .load()
    )
    
    parsed = raw.select(
        "timestamp", f.from_json(raw.value.cast("string"), json_schema).alias("json")
    ).select(
        f.col("timestamp").alias("proc_time"),
        f.col("json").getField("ID").alias("ID"),
        f.col("json").getField("Outstanding_Debt").alias("Outstanding_Debt"),
        f.col("json").getField("Credit_Mix").alias("Credit_Mix"),
        f.col("json").getField("Num_Credit_Card").alias("Num_Credit_Card"),
        f.col("json").getField("Interest_Rate").alias("Interest_Rate"),
    )
    
    decyzja_kredytowa_udf = f.udf(decyzja_kredytowa, ArrayType(StringType()))
    
    parsed = parsed.withColumn("Decision",decyzja_kredytowa_udf(
        f.col("Outstanding_Debt"), 
        f.col("Credit_Mix"),
        f.col("Num_Credit_Card"), 
        f.col("Interest_Rate")
    )
                              )
    
    # def generate_pdf(row):
    #     if row["Decision"][0] == "Good" or row["Decision"][0] == "Standard":
    #         pdf = FPDF()
    #         pdf.add_page()
    #         pdf.set_font("Arial", size=12)
    #         pdf.cell(0, 10, f"Kredyt przyznano osobie {row['ID']} ze stopa oprocentowania {row['Decision'][1]}", ln=True)
    #         pdf.output(f"{row['ID']}.pdf", "F")
     

    names = {}
    with open('IDnames.csv', 'r', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            names[row[0]] = row[1]
            
            
    def generate_pdf(row):
        if row["Decision"][0] == "Good" or row["Decision"][0] == "Standard":
            
            pozyczkobiorca_id = row['ID']
            imie_i_nazwisko = names[pozyczkobiorca_id]
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", size=12)
            
            pdf.image("logo.jpg", x=10, y=10, w=40)
            

            text = f"\n\n\n\n"
            text += f"                                                            Umowa o zawarcie kredytu\n\n"
            text += f"Strony umowy:\n"
            text += f"Pozyczkodawca: NaszSuperBank\n"
            text += f"Pozyczkobiorca: {imie_i_nazwisko}\n\n"
            text += f"Warunki kredytu:\n"
            text += f"Kwota kredytu: 1.000,00 PLN\n"
            text += f"Okres kredytowania: 3 miesiace\n"
            text += f"Stopa oprocentowania: {float(row['Decision'][1])*100}%\n\n"
            text += f"Zobowiazania pozyczkobiorcy:\n"
            text += f"1. Splacanie raty kredytu terminowo\n"
            text += f"2. Utrzymanie pozytywnej historii kredytowej\n\n"
            text += f"Zobowiazania pozyczkodawcy:\n"
            text += f"1. Udzielenie kredytu na okreslonych warunkach\n"
            text += f"2. Zapewnienie obslugi kredytu i informowania o ewentualnych zmianach\n\n\n\n"
            text += f"Podpis klienta: ________________                     Podpis reprezentanta banku: ________________"
            
            x = pdf.w - 50
            y = pdf.h - 40
            pdf.image("podpis.jpg", x=x, y=y, w=35)

            normalized_text = unicodedata.normalize("NFKD", text)
            pdf.multi_cell(0, 10, normalized_text, align="L")

            filename = f"{row['ID']}_umowa.pdf"
            pdf.output(f"umowy/{filename}")

    # defining output
    query = parsed.writeStream.outputMode("append").format("console").start()
    query1 = parsed.writeStream.foreach(generate_pdf).start()
    query.awaitTermination()
    query.stop()
    query1.awaitTermination()
    query1.stop()
    
    


