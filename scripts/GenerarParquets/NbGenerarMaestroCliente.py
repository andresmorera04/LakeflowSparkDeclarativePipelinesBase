# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # NbGenerarMaestroCliente — Generador del Parquet de Maestro de Clientes
# MAGIC
# MAGIC **Proposito**: Generar un archivo parquet simulando la tabla CMSTFL (Customer Master File)
# MAGIC de AS400 para una entidad bancaria.
# MAGIC
# MAGIC **Estructura**: 70 columnas (42 textuales, 18 fechas, 10 numericos) segun H2.1.
# MAGIC
# MAGIC **Volumetria**:
# MAGIC - Primera ejecucion: 5,000,000 de registros.
# MAGIC - Re-ejecucion: mismos clientes + mutacion del 20% en 15 campos demograficos + 0.60% clientes nuevos.
# MAGIC
# MAGIC **Nombres**: Exclusivamente hebreos, egipcios e ingleses (catalogos R3-D1).
# MAGIC
# MAGIC **Parametrizacion**: Todos los valores via `dbutils.widgets.text()` (Decision V2-R2-D1).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definicion de Parametros (17 Widgets)

# COMMAND ----------

# T004 — Definicion de 17 widgets via dbutils.widgets.text()
# Parametros de rutas y volumetria
dbutils.widgets.text("ruta_salida_parquet", "abfss://container@storageaccount.dfs.core.windows.net/landing/maestro_clientes", "Ruta de Salida del Parquet")
dbutils.widgets.text("cantidad_registros_base", "5000000", "Cantidad de Registros Base")
dbutils.widgets.text("pct_incremento", "0.006", "Porcentaje de Incremento (0.60%)")
dbutils.widgets.text("pct_mutacion", "0.20", "Porcentaje de Mutacion (20%)")
dbutils.widgets.text("offset_custid", "100000001", "Offset Inicial de CUSTID")
dbutils.widgets.text("ruta_parquet_existente", "", "Ruta del Parquet Existente (vacio para primera ejecucion)")
dbutils.widgets.text("num_particiones", "20", "Numero de Particiones del Parquet")

# Parametros de rangos monetarios
dbutils.widgets.text("rango_credlmt_min", "1000", "Rango Minimo Limite de Credito")
dbutils.widgets.text("rango_credlmt_max", "500000", "Rango Maximo Limite de Credito")
dbutils.widgets.text("rango_avlbal_min", "0", "Rango Minimo Saldo Disponible")
dbutils.widgets.text("rango_avlbal_max", "250000", "Rango Maximo Saldo Disponible")
dbutils.widgets.text("rango_income_min", "12000", "Rango Minimo Ingreso Anual")
dbutils.widgets.text("rango_income_max", "500000", "Rango Maximo Ingreso Anual")
dbutils.widgets.text("rango_loan_min", "0", "Rango Minimo Saldo Prestamo")
dbutils.widgets.text("rango_loan_max", "1000000", "Rango Maximo Saldo Prestamo")
dbutils.widgets.text("rango_ins_min", "0", "Rango Minimo Monto Seguro")
dbutils.widgets.text("rango_ins_max", "50000", "Rango Maximo Monto Seguro")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura y Validacion de Parametros (RF-016)

# COMMAND ----------

# T004 — Bloque de validacion de parametros
# Lectura de todos los parametros desde widgets
ruta_salida_parquet = dbutils.widgets.get("ruta_salida_parquet")
cantidad_registros_base = dbutils.widgets.get("cantidad_registros_base")
pct_incremento = dbutils.widgets.get("pct_incremento")
pct_mutacion = dbutils.widgets.get("pct_mutacion")
offset_custid = dbutils.widgets.get("offset_custid")
ruta_parquet_existente = dbutils.widgets.get("ruta_parquet_existente")
num_particiones = dbutils.widgets.get("num_particiones")
rango_credlmt_min = dbutils.widgets.get("rango_credlmt_min")
rango_credlmt_max = dbutils.widgets.get("rango_credlmt_max")
rango_avlbal_min = dbutils.widgets.get("rango_avlbal_min")
rango_avlbal_max = dbutils.widgets.get("rango_avlbal_max")
rango_income_min = dbutils.widgets.get("rango_income_min")
rango_income_max = dbutils.widgets.get("rango_income_max")
rango_loan_min = dbutils.widgets.get("rango_loan_min")
rango_loan_max = dbutils.widgets.get("rango_loan_max")
rango_ins_min = dbutils.widgets.get("rango_ins_min")
rango_ins_max = dbutils.widgets.get("rango_ins_max")

# Validacion de ruta de salida (obligatoria)
if not ruta_salida_parquet or ruta_salida_parquet.strip() == "":
    raise ValueError("ERROR: El parametro 'ruta_salida_parquet' no puede estar vacio.")

# Validacion de parametros numericos
try:
    cantidad_registros_base = int(cantidad_registros_base)
    assert cantidad_registros_base > 0, "Debe ser mayor a 0."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: 'cantidad_registros_base' debe ser un entero positivo. Detalle: {e}")

try:
    pct_incremento = float(pct_incremento)
    assert 0 <= pct_incremento <= 1, "Debe estar entre 0 y 1."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: 'pct_incremento' debe ser un numero entre 0 y 1. Detalle: {e}")

try:
    pct_mutacion = float(pct_mutacion)
    assert 0 <= pct_mutacion <= 1, "Debe estar entre 0 y 1."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: 'pct_mutacion' debe ser un numero entre 0 y 1. Detalle: {e}")

try:
    offset_custid = int(offset_custid)
    assert offset_custid > 0, "Debe ser mayor a 0."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: 'offset_custid' debe ser un entero positivo. Detalle: {e}")

try:
    num_particiones = int(num_particiones)
    assert num_particiones > 0, "Debe ser mayor a 0."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: 'num_particiones' debe ser un entero positivo. Detalle: {e}")

# Validacion de rangos monetarios
try:
    rango_credlmt_min = float(rango_credlmt_min)
    rango_credlmt_max = float(rango_credlmt_max)
    assert rango_credlmt_min <= rango_credlmt_max, "El minimo no puede ser mayor al maximo."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: Rango de limite de credito invalido. Detalle: {e}")

try:
    rango_avlbal_min = float(rango_avlbal_min)
    rango_avlbal_max = float(rango_avlbal_max)
    assert rango_avlbal_min <= rango_avlbal_max, "El minimo no puede ser mayor al maximo."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: Rango de saldo disponible invalido. Detalle: {e}")

try:
    rango_income_min = float(rango_income_min)
    rango_income_max = float(rango_income_max)
    assert rango_income_min <= rango_income_max, "El minimo no puede ser mayor al maximo."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: Rango de ingresos invalido. Detalle: {e}")

try:
    rango_loan_min = float(rango_loan_min)
    rango_loan_max = float(rango_loan_max)
    assert rango_loan_min <= rango_loan_max, "El minimo no puede ser mayor al maximo."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: Rango de prestamo invalido. Detalle: {e}")

try:
    rango_ins_min = float(rango_ins_min)
    rango_ins_max = float(rango_ins_max)
    assert rango_ins_min <= rango_ins_max, "El minimo no puede ser mayor al maximo."
except (ValueError, AssertionError) as e:
    raise ValueError(f"ERROR: Rango de seguro invalido. Detalle: {e}")

# Determinar si es primera ejecucion o re-ejecucion
es_primera_ejecucion = (ruta_parquet_existente is None or ruta_parquet_existente.strip() == "")

print("=" * 70)
print("PARAMETROS VALIDADOS EXITOSAMENTE")
print("=" * 70)
print(f"  Ruta de salida: {ruta_salida_parquet}")
print(f"  Cantidad base: {cantidad_registros_base:,}")
print(f"  Porcentaje incremento: {pct_incremento}")
print(f"  Porcentaje mutacion: {pct_mutacion}")
print(f"  Offset CUSTID: {offset_custid:,}")
print(f"  Particiones: {num_particiones}")
print(f"  Modo: {'PRIMERA EJECUCION' if es_primera_ejecucion else 'RE-EJECUCION'}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importaciones y Configuracion

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, DateType
)
from pyspark.sql import Row
import random
import math

# COMMAND ----------

# MAGIC %md
# MAGIC ## T005 — Definicion del Esquema StructType Explicito (70 Campos)

# COMMAND ----------

# Esquema StructType con 70 StructField segun H2.1 de specs/001-research-inicial-v1/research.md
# Distribucion: 42 StringType + 18 DateType + 8 LongType + 2 DoubleType = 70 campos
esquema_maestro_clientes = StructType([
    # --- Campos numericos (10 total) ---
    StructField("CUSTID", LongType(), False),       # 1. Identificador unico del cliente
    # --- Campos textuales (42 total) ---
    StructField("CUSTNM", StringType(), True),       # 2. Nombre completo del cliente (FRSTNM + LSTNM)
    StructField("FRSTNM", StringType(), True),       # 3. Primer nombre
    StructField("MDLNM", StringType(), True),        # 4. Segundo nombre
    StructField("LSTNM", StringType(), True),        # 5. Primer apellido
    StructField("SCNDLN", StringType(), True),       # 6. Segundo apellido
    StructField("GNDR", StringType(), True),         # 7. Genero (M/F)
    StructField("IDTYPE", StringType(), True),       # 8. Tipo de documento de identidad
    StructField("IDNMBR", StringType(), True),       # 9. Numero de documento de identidad
    StructField("NATNLT", StringType(), True),       # 10. Codigo de nacionalidad (ISO 3166)
    StructField("MRTLST", StringType(), True),       # 11. Estado civil (S/C/D/V)
    StructField("ADDR1", StringType(), True),        # 12. Direccion linea 1
    StructField("ADDR2", StringType(), True),        # 13. Direccion linea 2
    StructField("CITY", StringType(), True),         # 14. Ciudad
    StructField("STATE", StringType(), True),        # 15. Estado o provincia
    StructField("ZPCDE", StringType(), True),        # 16. Codigo postal
    StructField("CNTRY", StringType(), True),        # 17. Pais (ISO 3166)
    StructField("PHONE1", StringType(), True),       # 18. Telefono principal
    StructField("PHONE2", StringType(), True),       # 19. Telefono secundario
    StructField("EMAIL", StringType(), True),        # 20. Correo electronico
    StructField("OCCPTN", StringType(), True),       # 21. Ocupacion o profesion
    StructField("EMPLYR", StringType(), True),       # 22. Nombre del empleador
    StructField("EMPADS", StringType(), True),       # 23. Direccion del empleador
    StructField("BRNCOD", StringType(), True),       # 24. Codigo de sucursal de apertura
    StructField("BRNNM", StringType(), True),        # 25. Nombre de sucursal
    StructField("SGMNT", StringType(), True),        # 26. Segmento del cliente (VIP/PREM/STD/BAS)
    StructField("CSTCAT", StringType(), True),       # 27. Categoria del cliente
    StructField("RISKLV", StringType(), True),       # 28. Nivel de riesgo (01-05)
    StructField("PRDTYP", StringType(), True),       # 29. Tipo de producto principal
    StructField("ACCTST", StringType(), True),       # 30. Estado de la cuenta (AC/IN/CL/SU)
    StructField("TXID", StringType(), True),         # 31. Identificador fiscal
    StructField("LGLLNM", StringType(), True),       # 32. Nombre legal completo
    StructField("MTHNM", StringType(), True),        # 33. Nombre de la madre
    StructField("FTHNM", StringType(), True),        # 34. Nombre del padre
    StructField("CNTPRS", StringType(), True),       # 35. Persona de contacto de emergencia
    StructField("CNTPH", StringType(), True),        # 36. Telefono de contacto de emergencia
    StructField("PREFNM", StringType(), True),       # 37. Nombre preferido
    StructField("LANG", StringType(), True),         # 38. Idioma preferido
    StructField("EDLVL", StringType(), True),        # 39. Nivel educativo
    StructField("INCSRC", StringType(), True),       # 40. Fuente de ingresos
    StructField("RELTYP", StringType(), True),       # 41. Tipo de relacion bancaria
    StructField("NTFPRF", StringType(), True),       # 42. Preferencia de notificacion
    # --- Campos fecha (18 total) ---
    StructField("BRTDT", DateType(), True),          # 43. Fecha de nacimiento
    StructField("OPNDT", DateType(), True),          # 44. Fecha de apertura de la cuenta
    StructField("LSTTRX", DateType(), True),         # 45. Fecha de ultima transaccion
    StructField("LSTUPD", DateType(), True),         # 46. Fecha de ultima actualizacion
    StructField("CRTNDT", DateType(), True),         # 47. Fecha de creacion del registro
    StructField("EXPDT", DateType(), True),          # 48. Fecha de vencimiento de documento
    StructField("EMPSDT", DateType(), True),         # 49. Fecha de inicio de empleo
    StructField("LSTLGN", DateType(), True),         # 50. Fecha de ultimo login digital
    StructField("RVWDT", DateType(), True),          # 51. Fecha de revision KYC
    StructField("VLDDT", DateType(), True),          # 52. Fecha de validacion de datos
    StructField("ENRLDT", DateType(), True),         # 53. Fecha de enrolamiento digital
    StructField("CNCLDT", DateType(), True),         # 54. Fecha de cancelacion
    StructField("RJCTDT", DateType(), True),         # 55. Fecha de rechazo
    StructField("PRMDT", DateType(), True),          # 56. Fecha de promocion de segmento
    StructField("CHGDT", DateType(), True),          # 57. Fecha de ultimo cambio de estado
    StructField("LSTCDT", DateType(), True),         # 58. Fecha de ultimo contacto
    StructField("NXTRVW", DateType(), True),         # 59. Fecha de proxima revision
    StructField("BKRLDT", DateType(), True),         # 60. Fecha de relacion con el banco
    # --- Campos numericos restantes (9 de 10, excluyendo CUSTID ya definido) ---
    StructField("ANNLINC", DoubleType(), True),      # 61. Ingreso anual declarado
    StructField("MNTHINC", DoubleType(), True),      # 62. Ingreso mensual
    StructField("CRDSCR", LongType(), True),         # 63. Puntaje crediticio
    StructField("DPNDNT", LongType(), True),         # 64. Numero de dependientes
    StructField("TTLPRD", LongType(), True),         # 65. Total de productos contratados
    StructField("FNCLYR", LongType(), True),         # 66. Ano fiscal vigente
    StructField("AGECST", LongType(), True),         # 67. Edad del cliente
    StructField("YRBNKG", LongType(), True),         # 68. Anos de relacion bancaria
    StructField("RSKSCR", DoubleType(), True),       # 69. Score de riesgo calculado
    StructField("NUMPHN", LongType(), True),         # 70. Numero de telefonos registrados
])

print(f"Esquema definido con {len(esquema_maestro_clientes.fields)} campos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T006 — Catalogos de Nombres No Latinos (300 Nombres + 150 Apellidos)

# COMMAND ----------

# T006 — Catalogos hebreos, egipcios e ingleses aprobados en R3-D1
# 100 nombres hebreos (50 masculinos + 50 femeninos)
nombres_hebreos_masculinos = [
    "Abraham", "Adam", "Aharon", "Amiel", "Ariel", "Asher", "Avraham", "Baruch",
    "Benjamin", "Boaz", "Caleb", "Chaim", "Daniel", "David", "Efraim", "Elazar",
    "Eli", "Eliezer", "Elijah", "Emmanuel", "Ethan", "Ezra", "Gad", "Gideon",
    "Hillel", "Isaac", "Isaiah", "Israel", "Jacob", "Joel", "Jonathan", "Joseph",
    "Joshua", "Levi", "Malachi", "Meir", "Menachem", "Micah", "Mordechai", "Moses",
    "Nathan", "Nehemiah", "Noam", "Noah", "Raphael", "Samuel", "Seth", "Simeon",
    "Solomon", "Zev"
]

nombres_hebreos_femeninos = [
    "Abigail", "Adina", "Avital", "Batsheva", "Chana", "Dalia", "Deborah", "Devorah",
    "Dinah", "Edna", "Eliana", "Elisheva", "Esther", "Eve", "Gila", "Hadassah",
    "Hannah", "Ilana", "Judith", "Leah", "Liora", "Maia", "Margalit", "Michal",
    "Miriam", "Naomi", "Nava", "Noa", "Ofra", "Ora", "Penina", "Rachel",
    "Rebecca", "Rivka", "Ruth", "Sara", "Sarai", "Shira", "Shulamit", "Simcha",
    "Tamar", "Tali", "Tova", "Tzipporah", "Vered", "Yael", "Yehudit", "Zahava",
    "Zara", "Zippora"
]

# 100 nombres egipcios (50 masculinos + 50 femeninos)
nombres_egipcios_masculinos = [
    "Abasi", "Adio", "Akhenaten", "Amenhotep", "Ammon", "Anubis", "Asim", "Aten",
    "Azibo", "Badru", "Bomani", "Chenzira", "Chigaru", "Djoser", "Fenuku", "Gahiji",
    "Gyasi", "Hamadi", "Hanif", "Hasani", "Horus", "Imhotep", "Jabari", "Kafele",
    "Khalfani", "Khepri", "Kosey", "Lateef", "Maskini", "Menes", "Mensah", "Moswen",
    "Nabil", "Naguib", "Nkosi", "Nkrumah", "Oba", "Odion", "Okpara", "Omari",
    "Osiris", "Paki", "Quaashie", "Ramesses", "Rashidi", "Sadiki", "Sefu", "Thutmose",
    "Upuat", "Zahur"
]

nombres_egipcios_femeninos = [
    "Aisha", "Akila", "Amara", "Amunet", "Anippe", "Asenath", "Aziza", "Bahiti",
    "Bastet", "Bennu", "Chione", "Cleopatra", "Dalila", "Eboni", "Fayola", "Halima",
    "Hasina", "Hathor", "Hatshepsut", "Ife", "Isis", "Jamila", "Kakra", "Kamilah",
    "Kesi", "Kissa", "Layla", "Lotus", "Mandisa", "Masika", "Meret", "Nailah",
    "Nefertari", "Nefertiti", "Neith", "Nut", "Omorose", "Pandora", "Quibilah",
    "Rashida", "Safiya", "Sagira", "Sanura", "Selket", "Shani", "Siti", "Subira",
    "Taweret", "Umayma", "Zahrah"
]

# 100 nombres ingleses (50 masculinos + 50 femeninos)
nombres_ingleses_masculinos = [
    "Albert", "Alfred", "Andrew", "Arthur", "Benedict", "Charles", "Christopher",
    "Clement", "Colin", "Douglas", "Edmund", "Edward", "Frederick", "Geoffrey",
    "George", "Gerald", "Harold", "Henry", "Hugh", "James", "John", "Kenneth",
    "Lawrence", "Leonard", "Malcolm", "Martin", "Matthew", "Nicholas", "Oliver",
    "Patrick", "Paul", "Peter", "Philip", "Ralph", "Raymond", "Reginald", "Richard",
    "Robert", "Roger", "Roland", "Simon", "Stephen", "Stuart", "Theodore", "Thomas",
    "Timothy", "Vincent", "Walter", "William", "Winston"
]

nombres_ingleses_femeninos = [
    "Adelaide", "Alice", "Amelia", "Anne", "Beatrice", "Bridget", "Caroline",
    "Catherine", "Charlotte", "Clara", "Dorothy", "Eleanor", "Elizabeth", "Emily",
    "Emma", "Florence", "Frances", "Gertrude", "Grace", "Harriet", "Helen",
    "Isabella", "Jane", "Joan", "Julia", "Katherine", "Laura", "Lillian", "Louise",
    "Lucy", "Margaret", "Martha", "Mary", "Matilda", "Mildred", "Millicent", "Nora",
    "Olivia", "Penelope", "Philippa", "Rose", "Ruth", "Sarah", "Sophia", "Susan",
    "Teresa", "Victoria", "Violet", "Virginia", "Winifred"
]

# Consolidar 300 nombres (100 por cultura)
catalogo_nombres = (
    nombres_hebreos_masculinos + nombres_hebreos_femeninos +
    nombres_egipcios_masculinos + nombres_egipcios_femeninos +
    nombres_ingleses_masculinos + nombres_ingleses_femeninos
)

# 50 apellidos hebreos
apellidos_hebreos = [
    "Abramov", "Adler", "Ashkenazi", "Avidan", "Ben-Ari", "Ben-David", "Ben-Shimon",
    "Berman", "Blau", "Cohen", "Dayan", "Dror", "Eisen", "Elbaz", "Feldman",
    "Fischer", "Friedman", "Goldberg", "Goldman", "Grossman", "Halevi", "Katz",
    "Klein", "Levi", "Levin", "Levy", "Mizrahi", "Nir", "Ofer", "Peretz", "Rosen",
    "Rosenberg", "Rubin", "Schreiber", "Schwartz", "Shamir", "Shapira", "Sherman",
    "Shulman", "Silber", "Sofer", "Stern", "Strauss", "Talmor", "Weiss", "Wexler",
    "Yadin", "Yosef", "Zadok", "Zilber"
]

# 50 apellidos egipcios
apellidos_egipcios = [
    "Abdallah", "Abdelaziz", "Abdelrahman", "Aboutaleb", "Ahmed", "Amin", "Anwar",
    "Ashour", "Atef", "Bakr", "Darwish", "Desouki", "Eid", "Eldin", "Farag",
    "Fathi", "Gamal", "Ghali", "Habib", "Hamdi", "Hammad", "Haroun", "Hassan",
    "Hosni", "Hussein", "Ibrahim", "Ismail", "Kamel", "Kassem", "Khaled", "Lotfi",
    "Maher", "Mansour", "Moustafa", "Naguib", "Nasser", "Osman", "Qenawy", "Ragab",
    "Raafat", "Saad", "Sabry", "Salah", "Seif", "Shaheen", "Soliman", "Taha",
    "Tantawy", "Yousef", "Zaki"
]

# 50 apellidos ingleses
apellidos_ingleses = [
    "Abbott", "Baker", "Barnes", "Bennett", "Brooks", "Burton", "Campbell", "Chapman",
    "Clarke", "Cooper", "Davidson", "Edwards", "Fletcher", "Foster", "Graham", "Green",
    "Hall", "Harris", "Holmes", "Hughes", "Jackson", "Johnson", "King", "Lambert",
    "Lewis", "Marshall", "Miller", "Mitchell", "Moore", "Nelson", "Norton", "Palmer",
    "Parker", "Powell", "Reed", "Roberts", "Robinson", "Russell", "Scott", "Shaw",
    "Smith", "Spencer", "Taylor", "Thompson", "Turner", "Walker", "Ward", "Watson",
    "White", "Wilson"
]

# Consolidar 150 apellidos (50 por cultura)
catalogo_apellidos = apellidos_hebreos + apellidos_egipcios + apellidos_ingleses

print(f"Catalogos cargados: {len(catalogo_nombres)} nombres, {len(catalogo_apellidos)} apellidos.")
print(f"Combinaciones unicas posibles: {len(catalogo_nombres) * len(catalogo_apellidos):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T007 — Generacion de Registros: Primera Ejecucion

# COMMAND ----------

# Datos auxiliares para generacion de campos contextuales
# Todos estos valores son listas internas del generador (no hardcoded en los datos de salida)
lista_generos = ["M", "F"]
lista_tipos_documento = ["PS", "ID", "DL", "NI"]
lista_nacionalidades = ["ISR", "EGY", "GBR", "USA", "CAN", "AUS"]
lista_estados_civiles = ["S", "C", "D", "V"]
lista_ciudades = [
    "Jerusalem", "Tel Aviv", "Haifa", "Cairo", "Alexandria", "Giza",
    "London", "Manchester", "Birmingham", "Liverpool", "Leeds", "Bristol",
    "Edinburgh", "Glasgow", "Oxford", "Cambridge", "York", "Bath",
    "Luxor", "Aswan", "Sharm El Sheikh", "Hurghada", "Beersheba", "Eilat"
]
lista_estados = [
    "Central", "Northern", "Southern", "Tel Aviv", "Haifa", "Jerusalem",
    "Cairo", "Alexandria", "Giza", "Luxor", "Aswan", "Delta",
    "England", "Scotland", "Wales", "London", "Midlands", "Yorkshire"
]
lista_paises = ["ISR", "EGY", "GBR"]
lista_ocupaciones = [
    "Ingeniero", "Medico", "Abogado", "Profesor", "Contador", "Arquitecto",
    "Empresario", "Consultor", "Analista", "Gerente", "Director", "Investigador",
    "Programador", "Enfermero", "Farmaceutico", "Economista", "Psicologo", "Veterinario"
]
lista_empleadores = [
    "Tech Solutions Ltd", "Global Finance Corp", "Medical Center Inc",
    "Engineering Partners", "Legal Associates", "Education Foundation",
    "Construction Group", "Consulting Firm", "Analytics Corp", "Management Co",
    "Research Institute", "Software House", "Health Services", "Trade Company",
    "Energy Corp", "Transport Ltd", "Agriculture Co", "Media Group"
]
lista_sucursales = ["SUC001", "SUC002", "SUC003", "SUC004", "SUC005", "SUC006", "SUC007", "SUC008", "SUC009", "SUC010"]
lista_nombres_sucursal = [
    "Sucursal Central", "Sucursal Norte", "Sucursal Sur", "Sucursal Este",
    "Sucursal Oeste", "Sucursal Principal", "Sucursal Comercial",
    "Sucursal Industrial", "Sucursal Residencial", "Sucursal Digital"
]
lista_segmentos = ["VIP", "PREM", "STD", "BAS"]
lista_categorias = ["A01", "A02", "B01", "B02", "C01"]
lista_niveles_riesgo = ["01", "02", "03", "04", "05"]
lista_tipos_producto = ["AHRO", "CRTE", "PRES", "INVR"]
lista_estados_cuenta = ["AC", "IN", "CL", "SU"]
lista_idiomas = ["HE", "AR", "EN", "FR", "ES"]
lista_niveles_educacion = ["BSC", "MSC", "PHD", "DPL", "SEC"]
lista_fuentes_ingreso = ["EMPLEO", "NEGOCIO", "INVERSION", "PENSION", "HERENCIA"]
lista_tipos_relacion = ["PER", "EMP", "INS", "GOV"]
lista_preferencias_notificacion = ["EM", "SM", "PH", "ML"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funcion de Generacion de Registros con PySpark Distribuido

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType
import datetime

# Catalogos y parametros empaquetados en diccionarios Python para captura por closure.
# En Databricks Serverless no se soporta spark.sparkContext.broadcast()
# (JVM_ATTRIBUTE_NOT_SUPPORTED). Los diccionarios son serializados automaticamente
# por cloudpickle cuando mapInPandas distribuye la funcion a los workers.
datos_catalogos = {
    "nombres": catalogo_nombres,
    "apellidos": catalogo_apellidos,
    "generos": lista_generos,
    "tipos_doc": lista_tipos_documento,
    "nacionalidades": lista_nacionalidades,
    "estados_civiles": lista_estados_civiles,
    "ciudades": lista_ciudades,
    "estados": lista_estados,
    "paises": lista_paises,
    "ocupaciones": lista_ocupaciones,
    "empleadores": lista_empleadores,
    "sucursales": lista_sucursales,
    "nombres_sucursal": lista_nombres_sucursal,
    "segmentos": lista_segmentos,
    "categorias": lista_categorias,
    "niveles_riesgo": lista_niveles_riesgo,
    "tipos_producto": lista_tipos_producto,
    "estados_cuenta": lista_estados_cuenta,
    "idiomas": lista_idiomas,
    "niveles_educacion": lista_niveles_educacion,
    "fuentes_ingreso": lista_fuentes_ingreso,
    "tipos_relacion": lista_tipos_relacion,
    "pref_notif": lista_preferencias_notificacion,
}
datos_rangos = {
    "credlmt": (rango_credlmt_min, rango_credlmt_max),
    "avlbal": (rango_avlbal_min, rango_avlbal_max),
    "income": (rango_income_min, rango_income_max),
    "loan": (rango_loan_min, rango_loan_max),
    "ins": (rango_ins_min, rango_ins_max),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generacion Distribuida Usando mapInPandas / mapPartitions

# COMMAND ----------

import pandas as pd
import numpy as np

def generar_registros_maestro(iterador_particiones, offset_base, cantidad_total, num_parts):
    """
    Funcion generadora que produce registros del Maestro de Clientes.
    Se ejecuta en cada particion de forma distribuida.
    """
    nombres = datos_catalogos["nombres"]
    apellidos = datos_catalogos["apellidos"]
    generos = datos_catalogos["generos"]
    tipos_doc = datos_catalogos["tipos_doc"]
    nacionalidades = datos_catalogos["nacionalidades"]
    estados_civiles = datos_catalogos["estados_civiles"]
    ciudades = datos_catalogos["ciudades"]
    estados = datos_catalogos["estados"]
    paises = datos_catalogos["paises"]
    ocupaciones = datos_catalogos["ocupaciones"]
    empleadores = datos_catalogos["empleadores"]
    sucursales = datos_catalogos["sucursales"]
    nombres_suc = datos_catalogos["nombres_sucursal"]
    segmentos = datos_catalogos["segmentos"]
    categorias = datos_catalogos["categorias"]
    niveles_riesgo = datos_catalogos["niveles_riesgo"]
    tipos_producto = datos_catalogos["tipos_producto"]
    estados_cuenta = datos_catalogos["estados_cuenta"]
    idiomas = datos_catalogos["idiomas"]
    educacion = datos_catalogos["niveles_educacion"]
    fuentes = datos_catalogos["fuentes_ingreso"]
    tipos_rel = datos_catalogos["tipos_relacion"]
    pref_notif = datos_catalogos["pref_notif"]
    r_credlmt = datos_rangos["credlmt"]
    r_avlbal = datos_rangos["avlbal"]
    r_income = datos_rangos["income"]
    r_loan = datos_rangos["loan"]
    r_ins = datos_rangos["ins"]

    fecha_hoy = datetime.date.today()

    for pdf in iterador_particiones:
        # Cada particion recibe un dataframe con los indices de sus registros
        n = len(pdf)
        rng = np.random.default_rng()

        # Generar CUSTIDs secuenciales desde el offset de la particion
        custids = pdf["id_registro"].values + offset_base

        # Generar nombres del catalogo
        idx_frstnm = rng.integers(0, len(nombres), size=n)
        idx_mdlnm = rng.integers(0, len(nombres), size=n)
        idx_lstnm = rng.integers(0, len(apellidos), size=n)
        idx_scndln = rng.integers(0, len(apellidos), size=n)
        idx_mthnm = rng.integers(0, len(nombres), size=n)
        idx_fthnm = rng.integers(0, len(nombres), size=n)
        idx_prefnm = rng.integers(0, len(nombres), size=n)
        idx_cntprs_nombre = rng.integers(0, len(nombres), size=n)
        idx_cntprs_apellido = rng.integers(0, len(apellidos), size=n)

        frstnm = [nombres[i] for i in idx_frstnm]
        mdlnm = [nombres[i] for i in idx_mdlnm]
        lstnm = [apellidos[i] for i in idx_lstnm]
        scndln = [apellidos[i] for i in idx_scndln]
        custnm = [f"{frstnm[i]} {lstnm[i]}" for i in range(n)]
        mthnm = [nombres[i] for i in idx_mthnm]
        fthnm = [nombres[i] for i in idx_fthnm]
        prefnm = [nombres[i] for i in idx_prefnm]
        cntprs = [f"{nombres[idx_cntprs_nombre[i]]} {apellidos[idx_cntprs_apellido[i]]}" for i in range(n)]

        # Generar campos textuales contextuales
        gndr = rng.choice(generos, size=n).tolist()
        idtype = rng.choice(tipos_doc, size=n).tolist()
        idnmbr = [str(rng.integers(100000000, 999999999)) for _ in range(n)]
        natnlt = rng.choice(nacionalidades, size=n).tolist()
        mrtlst = rng.choice(estados_civiles, size=n).tolist()
        addr1 = [f"{rng.integers(1, 9999)} {rng.choice(ciudades)} St" for _ in range(n)]
        addr2 = [f"Apt {rng.integers(1, 999)}" if rng.random() > 0.5 else "" for _ in range(n)]
        city = rng.choice(ciudades, size=n).tolist()
        state = rng.choice(estados, size=n).tolist()
        zpcde = [str(rng.integers(10000, 99999)) for _ in range(n)]
        cntry = rng.choice(paises, size=n).tolist()
        phone1 = [f"+{rng.integers(1,999)}-{rng.integers(100,999)}-{rng.integers(1000000,9999999)}" for _ in range(n)]
        phone2 = [f"+{rng.integers(1,999)}-{rng.integers(100,999)}-{rng.integers(1000000,9999999)}" for _ in range(n)]
        dominios = ["mail.com", "email.co", "post.net", "inbox.org", "web.io"]
        email = [f"{frstnm[i].lower()}.{lstnm[i].lower()}@{rng.choice(dominios)}" for i in range(n)]
        occptn = rng.choice(ocupaciones, size=n).tolist()
        emplyr = rng.choice(empleadores, size=n).tolist()
        empads = [f"{rng.integers(1, 9999)} Business Ave, {rng.choice(ciudades)}" for _ in range(n)]
        brncod = rng.choice(sucursales, size=n).tolist()
        brnnm = rng.choice(nombres_suc, size=n).tolist()
        sgmnt = rng.choice(segmentos, size=n).tolist()
        cstcat = rng.choice(categorias, size=n).tolist()
        risklv = rng.choice(niveles_riesgo, size=n).tolist()
        prdtyp = rng.choice(tipos_producto, size=n).tolist()
        acctst = rng.choice(estados_cuenta, size=n).tolist()
        txid = [f"TX{rng.integers(100000000000, 999999999999)}" for _ in range(n)]
        lgllnm = [f"{frstnm[i]} {mdlnm[i]} {lstnm[i]} {scndln[i]}" for i in range(n)]
        cntph = [f"+{rng.integers(1,999)}-{rng.integers(100,999)}-{rng.integers(1000000,9999999)}" for _ in range(n)]
        lang = rng.choice(idiomas, size=n).tolist()
        edlvl = rng.choice(educacion, size=n).tolist()
        incsrc = rng.choice(fuentes, size=n).tolist()
        reltyp = rng.choice(tipos_rel, size=n).tolist()
        ntfprf = rng.choice(pref_notif, size=n).tolist()

        # Generar fechas
        def generar_fechas_aleatorias(n, dias_min, dias_max):
            """Genera n fechas aleatorias entre dias_min y dias_max dias atras desde hoy."""
            dias = rng.integers(dias_min, dias_max, size=n)
            return [fecha_hoy - datetime.timedelta(days=int(d)) for d in dias]

        brtdt = generar_fechas_aleatorias(n, 6570, 31025)      # 18-85 anos
        opndt = generar_fechas_aleatorias(n, 30, 10950)         # hasta 30 anos
        lsttrx = generar_fechas_aleatorias(n, 0, 730)           # ultimos 2 anos
        lstupd = generar_fechas_aleatorias(n, 0, 365)           # ultimo ano
        crtndt = generar_fechas_aleatorias(n, 30, 10950)        # hasta 30 anos
        expdt = [fecha_hoy + datetime.timedelta(days=int(rng.integers(30, 3650))) for _ in range(n)]
        empsdt = generar_fechas_aleatorias(n, 30, 14600)        # hasta 40 anos
        lstlgn = generar_fechas_aleatorias(n, 0, 90)            # ultimos 3 meses
        rvwdt = generar_fechas_aleatorias(n, 0, 365)            # ultimo ano
        vlddt = generar_fechas_aleatorias(n, 0, 365)            # ultimo ano
        enrldt = generar_fechas_aleatorias(n, 0, 3650)          # ultimos 10 anos
        cncldt = [fecha_hoy - datetime.timedelta(days=int(rng.integers(0, 3650))) if rng.random() > 0.9 else None for _ in range(n)]
        rjctdt = [fecha_hoy - datetime.timedelta(days=int(rng.integers(0, 3650))) if rng.random() > 0.95 else None for _ in range(n)]
        prmdt = generar_fechas_aleatorias(n, 0, 1825)           # ultimos 5 anos
        chgdt = generar_fechas_aleatorias(n, 0, 730)            # ultimos 2 anos
        lstcdt = generar_fechas_aleatorias(n, 0, 365)           # ultimo ano
        nxtrvw = [fecha_hoy + datetime.timedelta(days=int(rng.integers(30, 730))) for _ in range(n)]
        bkrldt = generar_fechas_aleatorias(n, 30, 10950)        # hasta 30 anos

        # Generar campos numericos con rangos monetarios parametrizables
        annlinc = rng.uniform(r_income[0], r_income[1], size=n).round(2).tolist()
        mnthinc = [round(a / 12.0, 2) for a in annlinc]
        crdscr = rng.integers(300, 850, size=n).tolist()
        dpndnt = rng.integers(0, 8, size=n).tolist()
        ttlprd = rng.integers(1, 15, size=n).tolist()
        fnclyr = [fecha_hoy.year] * n
        # Calcular edad del cliente a partir de la fecha de nacimiento
        agecst = [int((fecha_hoy - brtdt[i]).days / 365.25) for i in range(n)]
        yrbnkg = [int((fecha_hoy - bkrldt[i]).days / 365.25) for i in range(n)]
        rskscr = rng.uniform(0.0, 99.99, size=n).round(2).tolist()
        numphn = rng.integers(1, 5, size=n).tolist()

        # Construir DataFrame de pandas para la particion
        resultado = pd.DataFrame({
            "CUSTID": custids,
            "CUSTNM": custnm, "FRSTNM": frstnm, "MDLNM": mdlnm, "LSTNM": lstnm,
            "SCNDLN": scndln, "GNDR": gndr, "IDTYPE": idtype, "IDNMBR": idnmbr,
            "NATNLT": natnlt, "MRTLST": mrtlst, "ADDR1": addr1, "ADDR2": addr2,
            "CITY": city, "STATE": state, "ZPCDE": zpcde, "CNTRY": cntry,
            "PHONE1": phone1, "PHONE2": phone2, "EMAIL": email, "OCCPTN": occptn,
            "EMPLYR": emplyr, "EMPADS": empads, "BRNCOD": brncod, "BRNNM": brnnm,
            "SGMNT": sgmnt, "CSTCAT": cstcat, "RISKLV": risklv, "PRDTYP": prdtyp,
            "ACCTST": acctst, "TXID": txid, "LGLLNM": lgllnm, "MTHNM": mthnm,
            "FTHNM": fthnm, "CNTPRS": cntprs, "CNTPH": cntph, "PREFNM": prefnm,
            "LANG": lang, "EDLVL": edlvl, "INCSRC": incsrc, "RELTYP": reltyp,
            "NTFPRF": ntfprf,
            "BRTDT": brtdt, "OPNDT": opndt, "LSTTRX": lsttrx, "LSTUPD": lstupd,
            "CRTNDT": crtndt, "EXPDT": expdt, "EMPSDT": empsdt, "LSTLGN": lstlgn,
            "RVWDT": rvwdt, "VLDDT": vlddt, "ENRLDT": enrldt, "CNCLDT": cncldt,
            "RJCTDT": rjctdt, "PRMDT": prmdt, "CHGDT": chgdt, "LSTCDT": lstcdt,
            "NXTRVW": nxtrvw, "BKRLDT": bkrldt,
            "ANNLINC": annlinc, "MNTHINC": mnthinc, "CRDSCR": crdscr,
            "DPNDNT": dpndnt, "TTLPRD": ttlprd, "FNCLYR": fnclyr,
            "AGECST": agecst, "YRBNKG": yrbnkg, "RSKSCR": rskscr, "NUMPHN": numphn
        })

        yield resultado

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ejecutar Generacion de Datos

# COMMAND ----------

if es_primera_ejecucion:
    # T007 — Primera ejecucion: generar cantidad_registros_base registros
    print(f"Iniciando PRIMERA EJECUCION: generando {cantidad_registros_base:,} registros...")

    # Crear DataFrame base con indices secuenciales para distribuir generacion
    df_indices = spark.range(0, cantidad_registros_base).withColumnRenamed("id", "id_registro")

    # Generar registros usando mapInPandas para distribucion eficiente
    df_maestro = df_indices.repartition(num_particiones).mapInPandas(
        lambda iterador: generar_registros_maestro(iterador, offset_custid, cantidad_registros_base, num_particiones),
        schema=esquema_maestro_clientes
    )

    print(f"DataFrame generado con esquema de {len(esquema_maestro_clientes.fields)} columnas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T008 — Logica de Re-Ejecucion: Mutacion + Nuevos Clientes

# COMMAND ----------

if not es_primera_ejecucion:
    # T008 — Re-ejecucion: leer parquet existente, mutar 20%, agregar 0.60% nuevos
    print(f"Iniciando RE-EJECUCION desde: {ruta_parquet_existente}")

    # Leer parquet existente
    df_existente = spark.read.parquet(ruta_parquet_existente)
    total_existente = df_existente.count()
    custid_maximo = df_existente.agg(F.max("CUSTID")).collect()[0][0]
    print(f"  Registros existentes: {total_existente:,}")
    print(f"  CUSTID maximo existente: {custid_maximo:,}")

    # Separar registros: 20% para mutacion, 80% se mantienen sin cambios
    df_a_mutar = df_existente.sample(fraction=pct_mutacion, seed=42)
    df_sin_cambios = df_existente.subtract(df_a_mutar)
    cantidad_mutados = df_a_mutar.count()
    print(f"  Registros a mutar ({pct_mutacion*100:.0f}%): {cantidad_mutados:,}")

    # Aplicar mutaciones en los 15 campos demograficos
    # Campos a mutar: ADDR1, ADDR2, CITY, STATE, ZPCDE, PHONE1, PHONE2, EMAIL,
    #                  MRTLST, OCCPTN, EMPLYR, EMPADS, SGMNT, RISKLV, RSKSCR
    # Datos para mutacion capturados por closure (compatible con Serverless)
    ciudades_mut = list(lista_ciudades)
    estados_mut = list(lista_estados)
    ocupaciones_mut = list(lista_ocupaciones)
    empleadores_mut = list(lista_empleadores)

    def mutar_registros(iterador):
        """Mutar los 15 campos demograficos de los registros seleccionados."""
        ciudades_loc = ciudades_mut
        estados_loc = estados_mut
        ocupaciones_loc = ocupaciones_mut
        empleadores_loc = empleadores_mut
        estados_civiles_loc = ["S", "C", "D", "V"]
        segmentos_loc = ["VIP", "PREM", "STD", "BAS"]
        niveles_riesgo_loc = ["01", "02", "03", "04", "05"]
        dominios_loc = ["mail.com", "email.co", "post.net", "inbox.org", "web.io"]
        fecha_hoy_loc = datetime.date.today()

        for pdf in iterador:
            n = len(pdf)
            rng = np.random.default_rng()

            # Mutar los 15 campos
            pdf["ADDR1"] = [f"{rng.integers(1, 9999)} {rng.choice(ciudades_loc)} St" for _ in range(n)]
            pdf["ADDR2"] = [f"Apt {rng.integers(1, 999)}" if rng.random() > 0.5 else "" for _ in range(n)]
            pdf["CITY"] = rng.choice(ciudades_loc, size=n).tolist()
            pdf["STATE"] = rng.choice(estados_loc, size=n).tolist()
            pdf["ZPCDE"] = [str(rng.integers(10000, 99999)) for _ in range(n)]
            pdf["PHONE1"] = [f"+{rng.integers(1,999)}-{rng.integers(100,999)}-{rng.integers(1000000,9999999)}" for _ in range(n)]
            pdf["PHONE2"] = [f"+{rng.integers(1,999)}-{rng.integers(100,999)}-{rng.integers(1000000,9999999)}" for _ in range(n)]
            pdf["EMAIL"] = [f"{pdf['FRSTNM'].iloc[i].lower()}.{pdf['LSTNM'].iloc[i].lower()}.{rng.integers(1,9999)}@{rng.choice(dominios_loc)}" for i in range(n)]
            pdf["MRTLST"] = rng.choice(estados_civiles_loc, size=n).tolist()
            pdf["OCCPTN"] = rng.choice(ocupaciones_loc, size=n).tolist()
            pdf["EMPLYR"] = rng.choice(empleadores_loc, size=n).tolist()
            pdf["EMPADS"] = [f"{rng.integers(1, 9999)} Business Ave, {rng.choice(ciudades_loc)}" for _ in range(n)]
            pdf["SGMNT"] = rng.choice(segmentos_loc, size=n).tolist()
            pdf["RISKLV"] = rng.choice(niveles_riesgo_loc, size=n).tolist()
            pdf["RSKSCR"] = rng.uniform(0.0, 99.99, size=n).round(2).tolist()

            # Actualizar fecha de ultima actualizacion
            pdf["LSTUPD"] = [fecha_hoy_loc for _ in range(n)]

            yield pdf

    df_mutados = df_a_mutar.mapInPandas(mutar_registros, schema=esquema_maestro_clientes)

    # Generar nuevos clientes (0.60% sobre la base existente)
    cantidad_nuevos = round(total_existente * pct_incremento)
    nuevo_offset = custid_maximo + 1
    print(f"  Nuevos clientes ({pct_incremento*100:.2f}%): {cantidad_nuevos:,} con CUSTID desde {nuevo_offset:,}")

    # Crear DataFrame base para nuevos clientes
    df_indices_nuevos = spark.range(0, cantidad_nuevos).withColumnRenamed("id", "id_registro")

    # Generar nuevos registros reutilizando la misma funcion
    df_nuevos = df_indices_nuevos.repartition(max(1, num_particiones // 4)).mapInPandas(
        lambda iterador: generar_registros_maestro(iterador, nuevo_offset, cantidad_nuevos, max(1, num_particiones // 4)),
        schema=esquema_maestro_clientes
    )

    # Unir registros: sin cambios + mutados + nuevos
    df_maestro = df_sin_cambios.unionByName(df_mutados).unionByName(df_nuevos)
    total_final = total_existente + cantidad_nuevos
    print(f"  Total final esperado: {total_final:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## T009 — Escritura del Parquet

# COMMAND ----------

# T009 — Escritura con reparticion controlada y esquema StructType aplicado
print(f"Escribiendo parquet en: {ruta_salida_parquet}")
print(f"  Particiones: {num_particiones}")

df_maestro.repartition(num_particiones).write.mode("overwrite").parquet(ruta_salida_parquet)

# Verificacion post-escritura
df_verificacion = spark.read.parquet(ruta_salida_parquet)
total_escrito = df_verificacion.count()
columnas_escritas = len(df_verificacion.columns)

print("=" * 70)
print("GENERACION COMPLETADA EXITOSAMENTE")
print("=" * 70)
print(f"  Ruta: {ruta_salida_parquet}")
print(f"  Registros escritos: {total_escrito:,}")
print(f"  Columnas: {columnas_escritas}")
print(f"  Particiones: {num_particiones}")
print(f"  Modo: {'PRIMERA EJECUCION' if es_primera_ejecucion else 'RE-EJECUCION'}")
print("=" * 70)
