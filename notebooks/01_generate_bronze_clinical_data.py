# Databricks notebook source
# MAGIC %md
# MAGIC # CRISP bronze clinical data
# MAGIC
# MAGIC Generates a Maryland HIE-style bronze layer in `crisp.bronze` for the hybrid search demo
# MAGIC (Lakehouse Delta + later Lakebase sync).
# MAGIC
# MAGIC **Story:** 20% of patients (chronic high utilizers) drive ~80% of encounters. In March 2026,
# MAGIC **Frederick Health Hospital** had a lab-courier disruption. HbA1c, BMP, and BNP results were
# MAGIC delayed 3–14 days, follow-up diagnoses lagged, and those patients now have upcoming visits
# MAGIC that outreach teams need to find with hybrid search (name, ICD-10, lab comment, facility).
# MAGIC
# MAGIC | Table | Rows | Ties back to patients |
# MAGIC |---|---|---|
# MAGIC | `patients` | 2,000,000 | grain (`last_visited` in the last 5 years; 50% in the last 3) |
# MAGIC | `encounters` | 8,000,000 | `patient_id`; `embedding` = `chief_complaint` |
# MAGIC | `orders` | 8,000,000 | `patient_id`, `encounter_id` |
# MAGIC | `order_details` | 20,000,000 | `patient_id`, `order_id`; `embedding` = `test_name` + `result_comment` |
# MAGIC | `diagnoses` | 12,000,000 | `patient_id`, `encounter_id`; `embedding` = `diagnosis_description` + `clinical_note` |
# MAGIC
# MAGIC Run on **serverless** or a medium cluster. Expect ~10–20 minutes at full scale.

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "crisp")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("n_patients", "2000000")
dbutils.widgets.text("n_encounters", "8000000")
dbutils.widgets.text("n_orders", "8000000")
dbutils.widgets.text("n_order_details", "20000000")
dbutils.widgets.text("n_diagnoses", "12000000")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
N_PATIENTS = int(dbutils.widgets.get("n_patients"))
N_ENCOUNTERS = int(dbutils.widgets.get("n_encounters"))
N_ORDERS = int(dbutils.widgets.get("n_orders"))
N_DETAILS = int(dbutils.widgets.get("n_order_details"))
N_DX = int(dbutils.widgets.get("n_diagnoses"))
N_HIGH_UTIL = max(1, int(N_PATIENTS * 0.20))

TARGET = f"{CATALOG}.{SCHEMA}"
AS_OF = datetime(2026, 10, 21)

print(f"Writing to {TARGET} as of {AS_OF.date()}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {TARGET}
    COMMENT 'Bronze clinical source tables for the CRISP hybrid search demo'
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference dimensions

# COMMAND ----------

geo_df = spark.createDataFrame(
    [
        (0, "21201", "Baltimore", "MD"),
        (1, "21215", "Baltimore", "MD"),
        (2, "21224", "Baltimore", "MD"),
        (3, "20814", "Bethesda", "MD"),
        (4, "20850", "Rockville", "MD"),
        (5, "20910", "Silver Spring", "MD"),
        (6, "21044", "Columbia", "MD"),
        (7, "21401", "Annapolis", "MD"),
        (8, "21701", "Frederick", "MD"),
        (9, "21740", "Hagerstown", "MD"),
        (10, "21801", "Salisbury", "MD"),
        (11, "21204", "Towson", "MD"),
        (12, "21061", "Glen Burnie", "MD"),
        (13, "21117", "Owings Mills", "MD"),
        (14, "20646", "La Plata", "MD"),
    ],
    ["geo_idx", "zip_code", "city", "state"],
)

LAB_ROWS = [
        (0, "4548-4", "Hemoglobin A1c", "Blood", "numeric", "%", 5.8, 1.1, 4.0, 5.6, 3.5, 14.0),
        (1, "2339-0", "Glucose", "Blood", "numeric", "mg/dL", 108.0, 42.0, 70.0, 99.0, 40.0, 400.0),
        (2, "2160-0", "Creatinine", "Blood", "numeric", "mg/dL", 1.05, 0.45, 0.6, 1.3, 0.2, 8.0),
        (3, "3094-0", "BUN", "Blood", "numeric", "mg/dL", 16.0, 7.0, 7.0, 20.0, 2.0, 80.0),
        (4, "2947-0", "Sodium", "Blood", "numeric", "mmol/L", 139.0, 4.0, 136.0, 145.0, 120.0, 160.0),
        (5, "2823-3", "Potassium", "Blood", "numeric", "mmol/L", 4.2, 0.5, 3.5, 5.1, 2.5, 6.5),
        (6, "718-7", "Hemoglobin", "Blood", "numeric", "g/dL", 13.6, 1.8, 12.0, 17.5, 6.5, 20.0),
        (7, "6690-2", "WBC", "Blood", "numeric", "10*3/uL", 7.2, 3.1, 4.5, 11.0, 1.5, 30.0),
        (8, "777-3", "Platelets", "Blood", "numeric", "10*3/uL", 245.0, 70.0, 150.0, 400.0, 30.0, 900.0),
        (9, "2093-3", "Cholesterol total", "Blood", "numeric", "mg/dL", 188.0, 38.0, 0.0, 200.0, None, 400.0),
        (10, "2089-1", "LDL cholesterol", "Blood", "numeric", "mg/dL", 112.0, 32.0, 0.0, 100.0, None, 250.0),
        (11, "2085-9", "HDL cholesterol", "Blood", "numeric", "mg/dL", 52.0, 14.0, 40.0, 200.0, 15.0, None),
        (12, "2571-8", "Triglycerides", "Blood", "numeric", "mg/dL", 145.0, 80.0, 0.0, 150.0, None, 1000.0),
        (13, "3016-3", "TSH", "Blood", "numeric", "mIU/L", 2.1, 1.6, 0.4, 4.0, 0.01, 20.0),
        (14, "10839-9", "Troponin I", "Blood", "numeric", "ng/mL", 0.03, 0.4, 0.0, 0.04, None, 50.0),
        (15, "30934-4", "BNP", "Blood", "numeric", "pg/mL", 95.0, 220.0, 0.0, 100.0, None, 5000.0),
        (16, "1751-7", "Albumin", "Blood", "numeric", "g/dL", 4.1, 0.5, 3.5, 5.0, 2.0, 6.0),
        (17, "1742-6", "ALT", "Blood", "numeric", "U/L", 28.0, 22.0, 0.0, 40.0, None, 1000.0),
        (18, "1920-8", "AST", "Blood", "numeric", "U/L", 26.0, 20.0, 0.0, 40.0, None, 1000.0),
        (19, "5802-4", "Nitrite", "Urine", "qual", None, None, None, None, None, None, None),
        (20, "5799-2", "Leukocyte esterase", "Urine", "qual", None, None, None, None, None, None, None),
        (21, "94500-6", "SARS-CoV-2 RNA", "Nasopharyngeal swab", "qual", None, None, None, None, None, None, None),
        (22, "6301-6", "INR", "Blood", "numeric", "INR", 1.1, 0.4, 0.8, 1.1, 0.5, 8.0),
        (23, "8310-5", "Body temperature", "N/A", "numeric", "C", 37.0, 0.6, 36.1, 37.2, 34.0, 41.0),
        (24, "2345-7", "Glucose (fasting)", "Blood", "numeric", "mg/dL", 102.0, 28.0, 70.0, 99.0, 40.0, 400.0),
        (25, "789-8", "RBC", "Blood", "numeric", "10*6/uL", 4.7, 0.6, 4.2, 5.9, 2.0, 8.0),
        (26, "4544-3", "Hematocrit", "Blood", "numeric", "%", 41.0, 5.0, 36.0, 50.0, 18.0, 65.0),
        (27, "787-2", "MCV", "Blood", "numeric", "fL", 90.0, 6.0, 80.0, 100.0, 60.0, 130.0),
        (28, "770-8", "Neutrophils", "Blood", "numeric", "%", 58.0, 12.0, 40.0, 70.0, 10.0, 95.0),
        (29, "736-9", "Lymphocytes", "Blood", "numeric", "%", 28.0, 9.0, 20.0, 45.0, 5.0, 80.0),
        (30, "2075-0", "Chloride", "Blood", "numeric", "mmol/L", 102.0, 3.5, 96.0, 106.0, 80.0, 125.0),
        (31, "2028-9", "CO2", "Blood", "numeric", "mmol/L", 24.0, 3.0, 22.0, 29.0, 10.0, 45.0),
        (32, "17861-6", "Calcium", "Blood", "numeric", "mg/dL", 9.4, 0.5, 8.5, 10.5, 6.0, 14.0),
        (33, "19123-9", "Magnesium", "Blood", "numeric", "mg/dL", 2.0, 0.3, 1.7, 2.2, 0.8, 5.0),
        (34, "2777-1", "Phosphate", "Blood", "numeric", "mg/dL", 3.5, 0.7, 2.5, 4.5, 1.0, 9.0),
        (35, "48642-3", "eGFR", "Blood", "numeric", "mL/min/1.73m2", 78.0, 22.0, 60.0, 120.0, 5.0, None),
        (36, "6768-6", "Alkaline phosphatase", "Blood", "numeric", "U/L", 78.0, 30.0, 40.0, 129.0, None, 1000.0),
        (37, "1975-2", "Total bilirubin", "Blood", "numeric", "mg/dL", 0.8, 0.6, 0.1, 1.2, None, 20.0),
        (38, "2885-2", "Total protein", "Blood", "numeric", "g/dL", 7.0, 0.6, 6.0, 8.3, 3.5, 10.0),
        (39, "3024-7", "Free T4", "Blood", "numeric", "ng/dL", 1.2, 0.3, 0.8, 1.8, 0.2, 6.0),
        (40, "1989-3", "25-OH Vitamin D", "Blood", "numeric", "ng/mL", 28.0, 12.0, 30.0, 100.0, 5.0, 150.0),
        (41, "2132-9", "Vitamin B12", "Blood", "numeric", "pg/mL", 420.0, 180.0, 200.0, 900.0, 50.0, None),
        (42, "2276-4", "Ferritin", "Blood", "numeric", "ng/mL", 85.0, 90.0, 30.0, 400.0, 8.0, 2000.0),
        (43, "2498-4", "Iron", "Blood", "numeric", "ug/dL", 90.0, 35.0, 60.0, 170.0, 15.0, 400.0),
        (44, "33762-6", "NT-proBNP", "Blood", "numeric", "pg/mL", 140.0, 280.0, 0.0, 125.0, None, 20000.0),
        (45, "6598-7", "Troponin T", "Blood", "numeric", "ng/mL", 0.02, 0.3, 0.0, 0.01, None, 30.0),
        (46, "48065-7", "D-dimer", "Blood", "numeric", "ng/mL", 280.0, 400.0, 0.0, 500.0, None, 10000.0),
        (47, "2157-6", "CK", "Blood", "numeric", "U/L", 110.0, 80.0, 30.0, 200.0, None, 5000.0),
        (48, "5902-2", "Prothrombin time", "Blood", "numeric", "s", 12.2, 1.8, 11.0, 13.5, 8.0, 60.0),
        (49, "3173-2", "aPTT", "Blood", "numeric", "s", 30.0, 6.0, 25.0, 35.0, 15.0, 120.0),
        (50, "1988-5", "CRP", "Blood", "numeric", "mg/L", 4.5, 12.0, 0.0, 10.0, None, 300.0),
        (51, "2524-7", "Lactate", "Blood", "numeric", "mmol/L", 1.4, 1.1, 0.5, 2.0, None, 20.0),
        (52, "3084-1", "Uric acid", "Blood", "numeric", "mg/dL", 5.4, 1.6, 3.5, 7.2, None, 15.0),
        (53, "2857-1", "PSA", "Blood", "numeric", "ng/mL", 1.8, 2.4, 0.0, 4.0, None, 100.0),
        (54, "1798-8", "Amylase", "Blood", "numeric", "U/L", 70.0, 40.0, 30.0, 110.0, None, 2000.0),
        (55, "3040-3", "Lipase", "Blood", "numeric", "U/L", 45.0, 35.0, 10.0, 60.0, None, 3000.0),
        (56, "2324-2", "GGT", "Blood", "numeric", "U/L", 32.0, 28.0, 0.0, 50.0, None, 800.0),
        (57, "14957-5", "Microalbumin", "Urine", "numeric", "mg/L", 18.0, 25.0, 0.0, 30.0, None, 500.0),
        (58, "2888-6", "Urine protein", "Urine", "numeric", "mg/dL", 12.0, 20.0, 0.0, 20.0, None, 500.0),
        (59, "5811-5", "Urine specific gravity", "Urine", "numeric", None, 1.018, 0.008, 1.005, 1.030, 1.001, 1.050),
        (60, "5803-2", "Urine pH", "Urine", "numeric", None, 6.0, 0.8, 5.0, 8.0, 4.0, 9.0),
        (61, "5792-7", "Urine glucose", "Urine", "qual", None, None, None, None, None, None, None),
        (62, "5797-6", "Urine ketones", "Urine", "qual", None, None, None, None, None, None, None),
        (63, "2106-3", "Urine hCG", "Urine", "qual", None, None, None, None, None, None, None),
        (64, "2335-8", "Occult blood", "Stool", "qual", None, None, None, None, None, None, None),
        (65, "80382-5", "Influenza A RNA", "Nasopharyngeal swab", "qual", None, None, None, None, None, None, None),
        (66, "80383-3", "Influenza B RNA", "Nasopharyngeal swab", "qual", None, None, None, None, None, None, None),
        (67, "40988-1", "RSV RNA", "Nasopharyngeal swab", "qual", None, None, None, None, None, None, None),
        (68, "6557-3", "Group A Strep", "Throat swab", "qual", None, None, None, None, None, None, None),
        (69, "34713-8", "C. difficile toxin", "Stool", "qual", None, None, None, None, None, None, None),
        (70, "600-7", "Blood culture", "Blood", "qual", None, None, None, None, None, None, None),
        (71, "630-4", "Urine culture", "Urine", "qual", None, None, None, None, None, None, None),
        (72, "5196-1", "Hepatitis B surface antigen", "Blood", "qual", None, None, None, None, None, None, None),
        (73, "16128-1", "Hepatitis C antibody", "Blood", "qual", None, None, None, None, None, None, None),
        (74, "56888-1", "HIV 1/2 antibody", "Blood", "qual", None, None, None, None, None, None, None),
]
N_LAB_TESTS = len(LAB_ROWS)
assert N_LAB_TESTS == 75, N_LAB_TESTS
lab_df = spark.createDataFrame(
    LAB_ROWS,
    schema="""
        test_idx INT, loinc_code STRING, test_name STRING, specimen_type STRING,
        result_type STRING, result_unit STRING, typical_mean DOUBLE, typical_std DOUBLE,
        ref_low DOUBLE, ref_high DOUBLE, critical_low DOUBLE, critical_high DOUBLE
    """,
)

dx_df = spark.createDataFrame(
    [
        (0, "E11.9", "Type 2 diabetes mellitus without complications", "endocrine"),
        (1, "E11.22", "Type 2 diabetes mellitus with diabetic chronic kidney disease", "endocrine"),
        (2, "E11.65", "Type 2 diabetes mellitus with hyperglycemia", "endocrine"),
        (3, "I10", "Essential (primary) hypertension", "cardio"),
        (4, "I50.9", "Heart failure, unspecified", "cardio"),
        (5, "I50.22", "Chronic systolic (congestive) heart failure", "cardio"),
        (6, "I25.10", "Atherosclerotic heart disease of native coronary artery", "cardio"),
        (7, "N18.3", "Chronic kidney disease, stage 3 (moderate)", "renal"),
        (8, "N18.4", "Chronic kidney disease, stage 4 (severe)", "renal"),
        (9, "J44.1", "COPD with (acute) exacerbation", "pulmonary"),
        (10, "J45.909", "Unspecified asthma, uncomplicated", "pulmonary"),
        (11, "E78.5", "Hyperlipidemia, unspecified", "endocrine"),
        (12, "F32.9", "Major depressive disorder, single episode, unspecified", "behavioral"),
        (13, "F41.1", "Generalized anxiety disorder", "behavioral"),
        (14, "M54.5", "Low back pain", "ms"),
        (15, "M17.11", "Unilateral primary osteoarthritis, right knee", "ms"),
        (16, "E66.9", "Obesity, unspecified", "endocrine"),
        (17, "G47.33", "Obstructive sleep apnea (adult)", "pulmonary"),
        (18, "N39.0", "Urinary tract infection, site not specified", "infectious"),
        (19, "J18.9", "Pneumonia, unspecified organism", "infectious"),
        (20, "A41.9", "Sepsis, unspecified organism", "infectious"),
        (21, "I63.9", "Cerebral infarction, unspecified", "neuro"),
        (22, "G30.9", "Alzheimer's disease, unspecified", "neuro"),
        (23, "K21.0", "GERD with esophagitis", "gi"),
        (24, "K74.60", "Unspecified cirrhosis of liver", "gi"),
        (25, "C34.90", "Malignant neoplasm of unspecified part of bronchus or lung", "oncology"),
        (26, "C50.911", "Malignant neoplasm of unspecified site of right female breast", "oncology"),
        (27, "Z00.00", "Encounter for general adult medical examination without abnormal findings", "admin"),
        (28, "Z23", "Encounter for immunization", "admin"),
        (29, "R07.9", "Chest pain, unspecified", "symptom"),
        (30, "R06.02", "Shortness of breath", "symptom"),
        (31, "R51.9", "Headache, unspecified", "symptom"),
        (32, "Z79.4", "Long term (current) use of insulin", "endocrine"),
        (33, "Z87.891", "Personal history of nicotine dependence", "admin"),
        (34, "I48.91", "Unspecified atrial fibrillation", "cardio"),
    ],
    ["dx_idx", "icd10_code", "diagnosis_description", "dx_family"],
)

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
    "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
    "Frank", "Rachel", "Alexander", "Carolyn", "Patrick", "Janet", "Jack", "Catherine",
    "Dennis", "Maria", "Jerry", "Heather", "Tyler", "Diane", "Aaron", "Olivia",
    "Jose", "Julie", "Adam", "Joyce", "Nathan", "Victoria", "Henry", "Kelly",
    "Zachary", "Christina", "Douglas", "Lauren", "Peter", "Joan", "Kyle", "Evelyn",
    "Noah", "Judith", "Ethan", "Megan", "Jeremy", "Cheryl", "Walter", "Andrea",
    "Christian", "Hannah", "Keith", "Martha", "Roger", "Jacqueline", "Terry", "Frances",
    "Gerald", "Gloria", "Harold", "Ann", "Sean", "Teresa", "Austin", "Kathryn",
    "Carl", "Sara", "Arthur", "Janice", "Lawrence", "Jean", "Dylan", "Alice",
    "Jesse", "Madison", "Jordan", "Doris", "Bryan", "Abigail", "Billy", "Judy",
    "Joe", "Julia", "Bruce", "Grace", "Gabriel", "Denise", "Logan", "Amber",
    "Albert", "Marilyn", "Willie", "Beverly", "Alan", "Danielle", "Eugene", "Theresa",
    "Russell", "Sophia", "Vincent", "Marie", "Philip", "Diana", "Bobby", "Brittany",
    "Johnny", "Natalie", "Bradley", "Isabella", "Randy", "Charlotte", "Howard", "Rose",
    "Joel", "Diana", "Harry", "Kayla", "Ralph", "Alexis", "Roy", "Lori",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
    "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
    "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
    "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
    "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey",
    "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
    "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza",
    "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers",
    "Long", "Ross", "Foster", "Jimenez", "Powell", "Jenkins", "Perry", "Russell",
    "Sullivan", "Bell", "Coleman", "Butler", "Henderson", "Barnes", "Gonzales", "Fisher",
    "Vasquez", "Simmons", "Romero", "Jordan", "Patterson", "Alexander", "Hamilton", "Graham",
    "Reynolds", "Griffin", "Wallace", "Moreno", "West", "Cole", "Hayes", "Bryant",
    "Herrera", "Gibson", "Ellis", "Tran", "Medina", "Aguilar", "Stevens", "Murray",
    "Ford", "Castro", "Marshall", "Owens", "Harrison", "Fernandez", "Mcdonald", "Woods",
    "Washington", "Kennedy", "Wells", "Vargas", "Henry", "Chen", "Freeman", "Webb",
    "Tucker", "Guzman", "Burns", "Crawford", "Olson", "Simpson", "Porter", "Hunter",
    "Gordon", "Mendez", "Silva", "Shaw", "Snyder", "Mason", "Dixon", "Munoz",
    "Hunt", "Hicks", "Holmes", "Palmer", "Wagner", "Black", "Robertson", "Boyd",
    "Rose", "Stone", "Salazar", "Fox", "Warren", "Mills", "Meyer", "Rice",
    "Schmidt", "Garza", "Daniels", "Ferguson", "Nichols", "Stephens", "Soto", "Weaver",
    "Ryan", "Gardner", "Payne", "Grant", "Dunn", "Kelley", "Spencer", "Hawkins",
]
PROVIDERS = [
    "Dr. Amina Rahman", "Dr. Brian Okafor", "Dr. Claire Nguyen", "Dr. Diego Alvarez",
    "Dr. Elena Petrova", "Dr. Farid Hassan", "Dr. Grace Whitfield", "Dr. Hiro Tanaka",
    "Dr. Isabel Vargas", "Dr. James Whitaker", "Dr. Kavita Sharma", "Dr. Liam O'Connor",
    "Dr. Maya Ellison", "Dr. Noah Bergman", "Dr. Olivia Grant", "Dr. Priya Desai",
    "Dr. Quentin Brooks", "Dr. Rosa Delgado", "Dr. Samuel Chen", "Dr. Tessa Harlan",
    "Dr. Anika Bose", "Dr. Malik Okonkwo", "Dr. Sofia Lindgren", "Dr. Mateo Ruiz",
    "Dr. Naomi Feldman", "Dr. Omar Haddad", "Dr. Yuki Nakamura", "Dr. Leila Karimi",
    "Dr. Theo Papadopoulos", "Dr. Amara Diallo", "Dr. Hugo Bertrand", "Dr. Ines Moretti",
    "Dr. Callum Fraser", "Dr. Nia Okoye", "Dr. Soren Berg", "Dr. Aisha Mensah",
    "Dr. Luca Bianchi", "Dr. Mei Lin", "Dr. Ravi Kapoor", "Dr. Helena Novak",
    "Dr. Jonah Klein", "Dr. Fatima El-Sayed", "Dr. Anders Holm", "Dr. Camila Duarte",
    "Dr. Kenji Sato", "Dr. Bridget O'Malley", "Dr. Yusuf Demir", "Dr. Paloma Ortega",
    "Dr. Elias Cohen", "Dr. Zara Ahmed", "Dr. Nils Johansson", "Dr. Chiara Rossi",
    "Dr. Kwame Boateng", "Dr. Daria Sokolov", "Dr. Imani Brooks", "Dr. Ren Tanaka",
    "Dr. Sienna Walsh", "Dr. Aditya Mehta", "Dr. Noor Al-Farsi", "Dr. Mateo Silva",
    "Dr. Freya Andersen", "Dr. Hassan Idris", "Dr. Lila Moreau", "Dr. Tomasz Kowalski",
    "Dr. Amelie Dubois", "Dr. Javier Castillo", "Dr. Priyanka Iyer", "Dr. Owen Gallagher",
    "Dr. Hana Kim", "Dr. Malik Rahman", "Dr. Elsa Bergstrom", "Dr. Diego Santos",
    "Dr. Nadia Volkov", "Dr. Kofi Asante", "Dr. Isla MacLeod", "Dr. Reza Sharif",
    "Dr. Camille Laurent", "Dr. Arjun Patel", "Dr. Marisol Vega", "Dr. Eoin Brennan",
]
_provider_seen = set(PROVIDERS)
_i = 0
while len(PROVIDERS) < 200:
    fn = FIRST_NAMES[_i % len(FIRST_NAMES)]
    ln = LAST_NAMES[(_i * 17 + _i // len(FIRST_NAMES) * 3) % len(LAST_NAMES)]
    name = f"Dr. {fn} {ln}"
    if name not in _provider_seen:
        _provider_seen.add(name)
        PROVIDERS.append(name)
    _i += 1
del _provider_seen, _i


def pick_from(col, values, salt):
    n = len(values)
    return F.element_at(F.lit(values), (F.abs(F.hash(col, F.lit(salt))) % n) + 1)


def weighted_patient_idx(id_col, salt, seed):
    """80% of child rows map onto the first 20% of patients (high utilizers)."""
    r = F.rand(seed)
    high = F.abs(F.hash(id_col, F.lit(salt + "-h"))) % N_HIGH_UTIL
    rest = N_HIGH_UTIL + (F.abs(F.hash(id_col, F.lit(salt + "-r"))) % (N_PATIENTS - N_HIGH_UTIL))
    return F.when(r < 0.80, high).otherwise(rest).cast("long")


def facility_expr(rand_col):
    return (
        F.when(rand_col < 0.22, "Johns Hopkins Hospital")
        .when(rand_col < 0.40, "University of Maryland Medical Center")
        .when(rand_col < 0.52, "Mercy Medical Center")
        .when(rand_col < 0.62, "Sinai Hospital of Baltimore")
        .when(rand_col < 0.72, "Frederick Health Hospital")
        .when(rand_col < 0.80, "Suburban Hospital")
        .when(rand_col < 0.87, "Anne Arundel Medical Center")
        .when(rand_col < 0.93, "Holy Cross Hospital")
        .otherwise("TidalHealth Peninsula Regional")
    )


def patient_id_expr(idx_col):
    return F.concat(F.lit("PAT-"), F.lpad(idx_col.cast("string"), 7, "0"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Patients (2M)

# COMMAND ----------

patient_idx = F.col("id").cast("long")
sex = F.when(F.abs(F.hash(patient_idx, F.lit("sex"))) % 100 < 51, "F").otherwise("M")
geo_idx = F.abs(F.hash(patient_idx, F.lit("geo"))) % 15
# Random draws referenced by more than one branch must be materialized as columns first;
# otherwise each reference re-evaluates rand() and short-circuited CASE branches drift apart.
primary_facility = facility_expr(F.col("_facility_r"))
is_high_utilizer = patient_idx < F.lit(N_HIGH_UTIL)

# Older high utilizers: DOB skewed earlier so chronic disease prevalence is coherent.
dob_offset = F.when(
    is_high_utilizer,
    365 * 55 + (F.abs(F.hash(patient_idx, F.lit("dob"))) % (365 * 30)),
).otherwise(365 * 18 + (F.abs(F.hash(patient_idx, F.lit("dob"))) % (365 * 50)))

visit_roll = F.col("_visit_r")
next_visit = (
    F.when(visit_roll < 0.12, F.lit(None).cast("timestamp"))
    .when(
        visit_roll < 0.22,
        F.to_timestamp(F.date_add(F.lit(AS_OF.date()), -1 * (1 + (F.abs(F.hash(patient_idx, F.lit("miss"))) % 45)))),
    )
    .otherwise(
        F.to_timestamp(
            F.date_add(
                F.lit(AS_OF.date()),
                1 + (F.abs(F.hash(patient_idx, F.lit("fut"))) % F.when(is_high_utilizer, F.lit(21)).otherwise(F.lit(90))),
            )
        )
    )
)

# last_visited: all dates in (now-5y, now]; 50% in the past 3 years, 50% in years 3–5.
last_visit_bucket = F.rand(107)
last_visited_days = F.when(
    last_visit_bucket < 0.50,
    F.abs(F.hash(patient_idx, F.lit("lv-recent"))) % (365 * 3),
).otherwise(
    (365 * 3) + (F.abs(F.hash(patient_idx, F.lit("lv-older"))) % (365 * 2)),
)
last_visited = F.date_sub(F.lit(AS_OF.date()), last_visited_days.cast("int"))

insurance = (
    F.when(is_high_utilizer & (F.rand(11) < 0.45), "Medicare")
    .when(F.rand(12) < 0.28, "Medicare")
    .when(F.rand(13) < 0.46, "CareFirst BlueCross BlueShield")
    .when(F.rand(14) < 0.60, "Medicaid")
    .when(F.rand(15) < 0.74, "Kaiser Permanente")
    .when(F.rand(16) < 0.88, "UnitedHealthcare")
    .otherwise("Self-pay")
)

patients = (
    spark.range(0, N_PATIENTS, numPartitions=64)
    .withColumn("patient_idx", patient_idx)
    .withColumn("_facility_r", F.rand(103))
    .withColumn("_visit_r", F.rand(104))
    .withColumn("patient_id", patient_id_expr(F.col("patient_idx")))
    .withColumn("first_name", pick_from(F.col("patient_idx"), FIRST_NAMES, "fn"))
    .withColumn("last_name", pick_from(F.col("patient_idx"), LAST_NAMES, "ln"))
    .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .withColumn("date_of_birth", F.date_sub(F.lit(AS_OF.date()), dob_offset.cast("int")))
    .withColumn("sex", sex)
    .withColumn("geo_idx", geo_idx)
    .withColumn("insurance_plan", insurance)
    .withColumn("primary_facility", primary_facility)
    .withColumn("is_high_utilizer", is_high_utilizer)
    .withColumn("last_visited", last_visited)
    .withColumn("next_scheduled_visit", next_visit)
    .join(geo_df, "geo_idx", "left")
    .select(
        "patient_id",
        "first_name",
        "last_name",
        "full_name",
        "date_of_birth",
        "sex",
        "zip_code",
        "city",
        "state",
        "insurance_plan",
        "primary_facility",
        "last_visited",
        "next_scheduled_visit",
        "is_high_utilizer",
        "patient_idx",
    )
)

(
    patients.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.patients")
)
spark.sql(
    f"""
    ALTER TABLE {TARGET}.patients SET TBLPROPERTIES (
      'delta.enableChangeDataFeed' = 'true',
      'quality' = 'bronze'
    )
    """
)
spark.sql(f"ALTER TABLE {TARGET}.patients CLUSTER BY (patient_id)")
print(f"patients: {spark.table(f'{TARGET}.patients').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encounters (8M)

# COMMAND ----------

enc_id = F.col("id").cast("long")
enc_patient_idx = weighted_patient_idx(enc_id, "enc-pat", 201)
enc_facility = facility_expr(F.col("_facility_r"))

# 24 months of history, with a 3x visit spike at Frederick during the courier outage.
base_day = F.abs(F.hash(enc_id, F.lit("day"))) % 730
enc_date = F.date_sub(F.lit(AS_OF.date()), base_day.cast("int"))
outage_force = (F.col("encounter_facility") == "Frederick Health Hospital") & (F.rand(21) < 0.18)
enc_date = F.when(
    outage_force,
    F.date_add(F.lit("2026-03-01"), (F.abs(F.hash(enc_id, F.lit("outage-day"))) % 21).cast("int")),
).otherwise(enc_date)
enc_hour = F.abs(F.hash(enc_id, F.lit("hour"))) % 24
enc_min = F.abs(F.hash(enc_id, F.lit("min"))) % 60
enc_ts = F.to_timestamp(
    F.concat_ws(
        " ",
        enc_date.cast("string"),
        F.concat_ws(":", F.lpad(enc_hour.cast("string"), 2, "0"), F.lpad(enc_min.cast("string"), 2, "0"), F.lit("00")),
    )
)

enc_type_r = F.col("_type_r")
encounter_type = (
    F.when(enc_type_r < 0.38, "Outpatient")
    .when(enc_type_r < 0.58, "ED")
    .when(enc_type_r < 0.70, "Inpatient")
    .when(enc_type_r < 0.82, "Lab Draw")
    .when(enc_type_r < 0.92, "Telehealth")
    .otherwise("Urgent Care")
)

chief_complaint = (
    F.when(F.col("encounter_type") == "Lab Draw", "Routine laboratory collection")
    .when(enc_type_r < 0.12, "Chest pain")
    .when(enc_type_r < 0.22, "Shortness of breath")
    .when(enc_type_r < 0.32, "Diabetes follow-up")
    .when(enc_type_r < 0.42, "Hypertension follow-up")
    .when(enc_type_r < 0.50, "Abdominal pain")
    .when(enc_type_r < 0.58, "Fever and cough")
    .when(enc_type_r < 0.66, "Medication refill")
    .when(enc_type_r < 0.74, "Fall with injury")
    .when(enc_type_r < 0.82, "Altered mental status")
    .when(enc_type_r < 0.90, "Back pain")
    .otherwise("Wellness visit")
)

disposition = (
    F.when(F.col("encounter_type") == "Inpatient", F.when(F.rand(31) < 0.92, "Discharged to home").otherwise("Transferred"))
    .when(F.col("encounter_type") == "ED", F.when(F.rand(32) < 0.78, "Discharged to home").when(F.rand(33) < 0.95, "Admitted").otherwise("Left AMA"))
    .otherwise("Discharged to home")
)

encounters = (
    spark.range(0, N_ENCOUNTERS, numPartitions=128)
    .withColumn("encounter_idx", enc_id)
    .withColumn("patient_idx", enc_patient_idx)
    .withColumn("patient_id", patient_id_expr(F.col("patient_idx")))
    .withColumn(
        "encounter_id",
        F.concat(F.lit("ENC-"), F.lpad(F.col("encounter_idx").cast("string"), 8, "0")),
    )
    .withColumn("_facility_r", F.rand(105))
    .withColumn("_type_r", F.rand(106))
    .withColumn("encounter_facility", enc_facility)
    .withColumn("encounter_date", enc_ts)
    .withColumn("encounter_type", encounter_type)
    .withColumn("chief_complaint", chief_complaint)
    .withColumn("discharge_disposition", disposition)
    .withColumn("embedding", F.col("chief_complaint"))
    .select(
        "encounter_id",
        "patient_id",
        "encounter_date",
        "encounter_facility",
        "encounter_type",
        "chief_complaint",
        "discharge_disposition",
        "embedding",
        "encounter_idx",
        "patient_idx",
    )
)

(
    encounters.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.encounters")
)
spark.sql(
    f"""
    ALTER TABLE {TARGET}.encounters SET TBLPROPERTIES (
      'delta.enableChangeDataFeed' = 'true',
      'quality' = 'bronze'
    )
    """
)
spark.sql(f"ALTER TABLE {TARGET}.encounters CLUSTER BY (patient_id, encounter_date)")
print(f"encounters: {spark.table(f'{TARGET}.encounters').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders (8M) and order details (20M)
# MAGIC
# MAGIC Orders hang off encounters. Details carry specimen, result, and free-text comments used later for hybrid search.

# COMMAND ----------

ord_id = F.col("id").cast("long")
# Most orders attach to a real encounter; keep referential integrity via modulo on encounter_idx.
ord_enc_idx = F.abs(F.hash(ord_id, F.lit("ord-enc"))) % N_ENCOUNTERS

# Re-read encounter keys so facility / timestamps stay coherent (no cache).
enc_keys = spark.table(f"{TARGET}.encounters").select(
    "encounter_idx",
    F.col("encounter_id").alias("enc_id"),
    F.col("patient_id").alias("enc_patient_id"),
    F.col("patient_idx").alias("enc_patient_idx"),
    "encounter_date",
    "encounter_facility",
)

orders_raw = (
    spark.range(0, N_ORDERS, numPartitions=128)
    .withColumn("order_idx", ord_id)
    .withColumn("encounter_idx", ord_enc_idx)
    .join(enc_keys, "encounter_idx", "inner")
    .withColumn("patient_id", F.col("enc_patient_id"))
    .withColumn("patient_idx", F.col("enc_patient_idx"))
    .withColumn("order_id", F.concat(F.lit("ORD-"), F.lpad(F.col("order_idx").cast("string"), 8, "0")))
    .withColumn(
        "order_datetime",
        F.timestamp_seconds(F.unix_timestamp("encounter_date") + (F.abs(F.hash(F.col("order_idx"), F.lit("lag"))) % 7200)),
    )
    .withColumn("ordering_provider", pick_from(F.col("order_idx"), PROVIDERS, "prov"))
    .withColumn("ordering_facility", F.col("encounter_facility"))
    .withColumn(
        "is_courier_outage",
        (F.col("ordering_facility") == F.lit("Frederick Health Hospital"))
        & (F.col("order_datetime") >= F.lit("2026-03-01").cast("timestamp"))
        & (F.col("order_datetime") < F.lit("2026-03-22").cast("timestamp")),
    )
    .withColumn(
        "order_status",
        F.when(F.col("is_courier_outage") & (F.rand(41) < 0.22), "Pending")
        .when(F.rand(42) < 0.04, "Cancelled")
        .otherwise("Completed"),
    )
    .withColumn("order_type", F.lit("Laboratory"))
    .select(
        "order_id",
        "enc_id",
        "patient_id",
        "order_datetime",
        "ordering_provider",
        "order_type",
        "order_status",
        "ordering_facility",
        "is_courier_outage",
        "order_idx",
        "encounter_idx",
        "patient_idx",
    )
    .withColumnRenamed("enc_id", "encounter_id")
)

(
    orders_raw.drop("is_courier_outage", "order_idx", "encounter_idx", "patient_idx")
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.orders")
)
spark.sql(
    f"""
    ALTER TABLE {TARGET}.orders SET TBLPROPERTIES (
      'delta.enableChangeDataFeed' = 'true',
      'quality' = 'bronze'
    )
    """
)
spark.sql(f"ALTER TABLE {TARGET}.orders CLUSTER BY (patient_id, order_id)")
print(f"orders: {spark.table(f'{TARGET}.orders').count():,}")

# COMMAND ----------

order_keys = spark.table(f"{TARGET}.orders").select(
    "order_id",
    "encounter_id",
    "patient_id",
    "order_datetime",
    "ordering_facility",
    "order_status",
).withColumn(
    "order_idx",
    F.col("order_id").substr(5, 8).cast("long"),
).withColumn(
    "is_courier_outage",
    (F.col("ordering_facility") == F.lit("Frederick Health Hospital"))
    & (F.col("order_datetime") >= F.lit("2026-03-01").cast("timestamp"))
    & (F.col("order_datetime") < F.lit("2026-03-22").cast("timestamp")),
)

# Weighted test mix: metabolic / A1c / cardiac markers dominate (searchable delayed-result story).
detail_id = F.col("id").cast("long")
test_r = F.col("_test_r")
test_idx = (
    F.when(test_r < 0.12, 0)  # A1c
    .when(test_r < 0.22, 1)  # glucose
    .when(test_r < 0.30, 2)  # creatinine
    .when(test_r < 0.36, 15)  # BNP
    .when(test_r < 0.42, 14)  # troponin
    .when(test_r < 0.50, 6)  # hemoglobin
    .when(test_r < 0.56, 4)  # sodium
    .when(test_r < 0.62, 5)  # potassium
    .when(test_r < 0.68, 9)  # cholesterol
    .otherwise((F.abs(F.hash(detail_id, F.lit("test-tail"))) % N_LAB_TESTS))
).cast("int")

details_raw = (
    spark.range(0, N_DETAILS, numPartitions=256)
    .withColumn("order_detail_idx", detail_id)
    .withColumn("order_idx", F.abs(F.hash(detail_id, F.lit("det-ord"))) % N_ORDERS)
    .withColumn("_test_r", F.rand(108))
    .withColumn("test_idx", test_idx)
    .join(lab_df, "test_idx", "left")
    .join(order_keys, "order_idx", "inner")
    .withColumn(
        "order_detail_id",
        F.concat(F.lit("ODT-"), F.lpad(F.col("order_detail_idx").cast("string"), 9, "0")),
    )
    .withColumn(
        "sample_id",
        F.concat(F.lit("SMP-"), F.lpad((F.abs(F.hash(F.col("order_detail_idx"), F.lit("smp"))) % 1000000000).cast("string"), 9, "0")),
    )
    .withColumn(
        "collected_at",
        F.timestamp_seconds(F.unix_timestamp("order_datetime") + (180 + (F.abs(F.hash(F.col("order_detail_idx"), F.lit("col"))) % 7200))),
    )
    .withColumn(
        "tat_seconds",
        F.when(
            F.col("is_courier_outage"),
            (3 * 86400) + (F.abs(F.hash(F.col("order_detail_idx"), F.lit("tat"))) % (11 * 86400)),
        ).otherwise(2 * 3600 + (F.abs(F.hash(F.col("order_detail_idx"), F.lit("tat"))) % (22 * 3600))),
    )
    .withColumn(
        "has_result",
        ~F.col("order_status").isin("Cancelled", "Pending"),
    )
    .withColumn(
        "resulted_at",
        F.when(F.col("has_result"), F.timestamp_seconds(F.unix_timestamp("collected_at") + F.col("tat_seconds"))),
    )
)

numeric_value = F.col("typical_mean") + (F.col("typical_std") * F.col("_z"))
# High utilizers skew metabolic markers up so diabetes/CKD is discoverable in results.
numeric_value = F.when(
    (F.col("patient_id").substr(5, 7).cast("long") < F.lit(N_HIGH_UTIL))
    & F.col("test_name").isin(
        "Hemoglobin A1c",
        "Glucose",
        "Glucose (fasting)",
        "Creatinine",
        "BNP",
        "NT-proBNP",
        "eGFR",
        "Microalbumin",
    ),
    numeric_value + F.col("typical_std"),
).otherwise(numeric_value)

qual_value = (
    F.when(
        F.col("test_name").isin(
            "SARS-CoV-2 RNA",
            "Influenza A RNA",
            "Influenza B RNA",
            "RSV RNA",
        ),
        F.when(F.rand(51) < 0.07, "Detected").otherwise("Not detected"),
    )
    .when(
        F.col("test_name").isin("Blood culture", "Urine culture"),
        F.when(F.rand(54) < 0.08, "Growth").otherwise("No growth"),
    )
    .when(F.rand(52) < 0.12, "Positive")
    .otherwise("Negative")
)

result_num = F.round(numeric_value, 2)
result_flag = (
    F.when(
        F.col("result_type") != "numeric",
        F.when(F.col("qual_value").isin("Positive", "Detected", "Growth"), "Abnormal").otherwise("Normal"),
    )
    .when(F.col("critical_low").isNotNull() & (F.col("result_num") <= F.col("critical_low")), "Critical")
    .when(F.col("critical_high").isNotNull() & (F.col("result_num") >= F.col("critical_high")), "Critical")
    .when(F.col("ref_low").isNotNull() & (F.col("result_num") < F.col("ref_low")), "Low")
    .when(F.col("ref_high").isNotNull() & (F.col("result_num") > F.col("ref_high")), "High")
    .otherwise("Normal")
)

result_comment = (
    F.when(
        F.col("is_courier_outage") & (F.col("order_status") == "Pending"),
        F.concat(
            F.lit("Result delayed due to Frederick Health courier disruption. Specimen "),
            F.col("sample_id"),
            F.lit(" still in transit from "),
            F.col("ordering_facility"),
            F.lit(". Repeat draw may be required before next scheduled visit."),
        ),
    )
    .when(
        F.col("is_courier_outage"),
        F.concat(
            F.lit("Late result after Frederick Health courier disruption. "),
            F.col("test_name"),
            F.lit(" reported "),
            F.col("result_flag_raw"),
            F.lit(". Correlate with diabetes and heart-failure follow-up."),
        ),
    )
    .when(
        F.col("result_flag_raw") == "Critical",
        F.concat(F.lit("Critical "), F.col("test_name"), F.lit(" — called to ordering provider at "), F.col("ordering_facility"), F.lit(".")),
    )
    .when(
        (F.col("test_name") == "Hemoglobin A1c") & (F.col("result_flag_raw").isin("High", "Critical")),
        F.lit("Elevated A1c consistent with suboptimal glycemic control. Recommend endocrine follow-up."),
    )
    .when(F.col("order_status") == "Cancelled", F.lit("Order cancelled; specimen not processed."))
    .otherwise(F.concat(F.lit("Routine "), F.col("test_name"), F.lit(" on "), F.col("specimen_type"), F.lit(" specimen.")))
)

order_details = (
    details_raw.withColumn("_z", F.randn(53))
    .withColumn("result_num", result_num)
    .withColumn("qual_value", qual_value)
    .withColumn("result_flag_raw", result_flag)
    .withColumn("result_comment", result_comment)
    .select(
        "order_detail_id",
        "order_id",
        "patient_id",
        "encounter_id",
        "loinc_code",
        "test_name",
        "specimen_type",
        "sample_id",
        "collected_at",
        "resulted_at",
        # Cancelled and Pending orders have no result yet.
        F.when(~F.col("has_result"), F.lit(None).cast("string"))
        .when(F.col("result_type") == "qual", F.col("qual_value"))
        .otherwise(F.col("result_num").cast("string"))
        .alias("result_value"),
        "result_unit",
        F.when(F.col("has_result"), F.col("result_flag_raw")).alias("result_flag"),
        "result_comment",
        F.concat_ws(". ", F.col("test_name"), F.col("result_comment")).alias("embedding"),
    )
)

(
    order_details.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.order_details")
)
spark.sql(
    f"""
    ALTER TABLE {TARGET}.order_details SET TBLPROPERTIES (
      'delta.enableChangeDataFeed' = 'true',
      'quality' = 'bronze'
    )
    """
)
spark.sql(f"ALTER TABLE {TARGET}.order_details CLUSTER BY (patient_id, order_id)")
print(f"order_details: {spark.table(f'{TARGET}.order_details').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnoses (12M)
# MAGIC
# MAGIC Multiple diagnoses per encounter. High utilizers concentrate in diabetes, CKD, and heart failure — the same cohort hit by delayed Frederick labs.

# COMMAND ----------

dx_id = F.col("id").cast("long")
dx_r = F.col("_dx_r")
# High-utilizer rows (patient_idx < N_HIGH_UTIL) get more chronic disease codes.
dx_idx = (
    F.when(dx_r < 0.10, 0)
    .when(dx_r < 0.16, 1)
    .when(dx_r < 0.22, 3)
    .when(dx_r < 0.27, 4)
    .when(dx_r < 0.31, 7)
    .when(dx_r < 0.35, 11)
    .when(dx_r < 0.39, 16)
    .when(dx_r < 0.43, 12)
    .when(dx_r < 0.47, 9)
    .when(dx_r < 0.51, 27)
    .when(dx_r < 0.55, 29)
    .when(dx_r < 0.59, 18)
    .otherwise((F.abs(F.hash(dx_id, F.lit("dx-tail"))) % 35))
).cast("int")

enc_dx_keys = spark.table(f"{TARGET}.encounters").select(
    "encounter_idx",
    "encounter_id",
    "patient_id",
    "patient_idx",
    "encounter_date",
    "encounter_facility",
    "chief_complaint",
)

diagnoses = (
    spark.range(0, N_DX, numPartitions=128)
    .withColumn("diagnosis_idx", dx_id)
    .withColumn("_dx_r", F.rand(109))
    .withColumn("encounter_idx", F.abs(F.hash(dx_id, F.lit("dx-enc"))) % N_ENCOUNTERS)
    .withColumn("dx_idx", dx_idx)
    .join(dx_df, "dx_idx", "left")
    .join(enc_dx_keys, "encounter_idx", "inner")
    .withColumn(
        "diagnosis_id",
        F.concat(F.lit("DX-"), F.lpad(F.col("diagnosis_idx").cast("string"), 8, "0")),
    )
    .withColumn("diagnosis_date", F.to_date("encounter_date"))
    .withColumn(
        "diagnosis_type",
        F.when(F.abs(F.hash(F.col("diagnosis_idx"), F.lit("pri"))) % 100 < 35, "Primary").otherwise("Secondary"),
    )
    .withColumn(
        "clinical_note",
        F.concat(
            F.lit("At "),
            F.col("encounter_facility"),
            F.lit(" the patient was evaluated for "),
            F.col("chief_complaint"),
            F.lit(". Assessment includes "),
            F.col("diagnosis_description"),
            F.lit(" ("),
            F.col("icd10_code"),
            F.lit("). "),
            F.when(
                F.col("icd10_code").startswith("E11")
                & (F.col("encounter_facility") == "Frederick Health Hospital"),
                F.lit(
                    "Glycemic follow-up may be incomplete because of delayed HbA1c and glucose results "
                    "during the March 2026 courier disruption. Review pending labs before the next scheduled visit."
                ),
            )
            .when(
                F.col("icd10_code").startswith("I50"),
                F.lit("Volume status and BNP trend should be reviewed; outreach if the next visit is within 21 days."),
            )
            .when(
                F.col("icd10_code").startswith("N18"),
                F.lit("Monitor creatinine trajectory and medication dosing. Coordinate with nephrology if stage 4."),
            )
            .otherwise(F.lit("Continue current care plan and reconcile medications at follow-up.")),
        ),
    )
    .withColumn(
        "embedding",
        F.concat_ws(". ", F.col("diagnosis_description"), F.col("clinical_note")),
    )
    .select(
        "diagnosis_id",
        "encounter_id",
        "patient_id",
        "diagnosis_date",
        "icd10_code",
        "diagnosis_description",
        "diagnosis_type",
        "dx_family",
        "clinical_note",
        "embedding",
    )
)

(
    diagnoses.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET}.diagnoses")
)
spark.sql(
    f"""
    ALTER TABLE {TARGET}.diagnoses SET TBLPROPERTIES (
      'delta.enableChangeDataFeed' = 'true',
      'quality' = 'bronze'
    )
    """
)
spark.sql(f"ALTER TABLE {TARGET}.diagnoses CLUSTER BY (patient_id, icd10_code)")
print(f"diagnoses: {spark.table(f'{TARGET}.diagnoses').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comments, demo view, and validation

# COMMAND ----------

spark.sql(
    f"COMMENT ON TABLE {TARGET}.patients IS 'Patient master: identity, DOB, last_visited, next visit. Grain: patient_id.'"
)
spark.sql(
    f"COMMENT ON TABLE {TARGET}.encounters IS 'Facility visits. embedding = chief_complaint. FK: patient_id → patients.patient_id.'"
)
spark.sql(f"COMMENT ON TABLE {TARGET}.orders IS 'Laboratory orders. FK: patient_id, encounter_id.'")
spark.sql(
    f"COMMENT ON TABLE {TARGET}.order_details IS 'Lab tests, specimens, results. embedding = test_name + result_comment. FK: order_id, patient_id.'"
)
spark.sql(
    f"COMMENT ON TABLE {TARGET}.diagnoses IS 'Encounter diagnoses. embedding = diagnosis_description + clinical_note. FK: encounter_id, patient_id.'"
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {TARGET}.v_frederick_delayed_labs
    COMMENT 'Outreach set: delayed Frederick labs joined to upcoming visits — hybrid search seed'
    AS
    SELECT
      p.patient_id,
      p.full_name,
      p.date_of_birth,
      p.last_visited,
      p.next_scheduled_visit,
      p.primary_facility,
      o.order_id,
      o.order_datetime,
      o.ordering_facility,
      d.test_name,
      d.sample_id,
      d.result_flag,
      d.result_comment,
      dx.icd10_code,
      dx.diagnosis_description
    FROM {TARGET}.order_details d
    JOIN {TARGET}.orders o ON d.order_id = o.order_id
    JOIN {TARGET}.patients p ON d.patient_id = p.patient_id
    LEFT JOIN {TARGET}.diagnoses dx
      ON dx.patient_id = p.patient_id
     AND dx.icd10_code LIKE 'E11%'
    WHERE o.ordering_facility = 'Frederick Health Hospital'
      AND o.order_datetime >= TIMESTAMP '2026-03-01'
      AND o.order_datetime < TIMESTAMP '2026-03-22'
      AND (
        d.result_comment LIKE '%courier disruption%'
        OR o.order_status = 'Pending'
      )
    """
)

# COMMAND ----------

counts = spark.sql(
    f"""
    SELECT 'patients' AS table_name, COUNT(*) AS row_count FROM {TARGET}.patients
    UNION ALL SELECT 'encounters', COUNT(*) FROM {TARGET}.encounters
    UNION ALL SELECT 'orders', COUNT(*) FROM {TARGET}.orders
    UNION ALL SELECT 'order_details', COUNT(*) FROM {TARGET}.order_details
    UNION ALL SELECT 'diagnoses', COUNT(*) FROM {TARGET}.diagnoses
    """
)
counts.show(truncate=False)

print("orphan encounters (should be 0):")
spark.sql(
    f"""
    SELECT COUNT(*) AS orphan_encounters
    FROM {TARGET}.encounters e
    LEFT ANTI JOIN {TARGET}.patients p ON e.patient_id = p.patient_id
    """
).show()

print("orphan order_details (should be 0):")
spark.sql(
    f"""
    SELECT COUNT(*) AS orphan_details
    FROM {TARGET}.order_details d
    LEFT ANTI JOIN {TARGET}.orders o ON d.order_id = o.order_id
    """
).show()

print("orphan diagnoses (should be 0):")
spark.sql(
    f"""
    SELECT COUNT(*) AS orphan_dx
    FROM {TARGET}.diagnoses dx
    LEFT ANTI JOIN {TARGET}.encounters e ON dx.encounter_id = e.encounter_id
    """
).show()

print("Frederick outage slice:")
spark.sql(
    f"""
    SELECT COUNT(*) AS delayed_detail_rows
    FROM {TARGET}.order_details
    WHERE result_comment LIKE '%courier disruption%'
    """
).show()

print("last_visited window (expect ~50% in past 3 years, all within 5 years):")
spark.sql(
    f"""
    SELECT
      COUNT(*) AS patients,
      SUM(CASE WHEN last_visited BETWEEN date_sub(DATE '{AS_OF.date()}', 365 * 5) AND DATE '{AS_OF.date()}' THEN 1 ELSE 0 END) AS within_5y,
      ROUND(AVG(CASE WHEN last_visited >= date_sub(DATE '{AS_OF.date()}', 365 * 3) THEN 1.0 ELSE 0.0 END), 3) AS pct_within_3y,
      MIN(last_visited) AS min_last_visited,
      MAX(last_visited) AS max_last_visited
    FROM {TARGET}.patients
    """
).show()

print("Done.")
