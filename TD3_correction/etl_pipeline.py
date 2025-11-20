import pandas as pd # https://pandas.pydata.org/docs/index.html#
import logging # https://realpython.com/python-logging/
import pandera.pandas as pa # https://pandera.readthedocs.io/en/latest/dataframe_schemas.html
import datetime # https://www.w3schools.com/python/python_datetime.asp
import json 


# formattage des loggings + ajout du fichier app.log pour enregistrer les logs avec la méthode basicConfig
logging.basicConfig(
    # filename = "app.log",
    # encoding = "utf-8",
    # filemode = "a",
     format = "{asctime} - {levelname} - {message}",
     style = "{",
     datefmt = "%Y-%m-%d %H:%M:%S",
     level = logging.INFO # inportant de l'indiquer car par défaut, seulement à partir du niveau WARNING
 )

# fonction d'ingestion
def extract_data() :
    filepath = r"data\input"
    filename = r"\users_raw.csv"
    try : 
        logging.info("Start reading the CSV file")
        df = pd.read_csv(filepath+filename, sep = ',')
        logging.info("CSV file read with success")
        return df
    except Exception as e:
         logging.error(f"CSV file error : {e}")
         return 

# fonction de transformation
def transform_data(df) :
    logging.info("Début transformation des données")
    df['age'] = df['age'].apply(lambda x : 0 if x < 1 else x)
    df['signup_date'] = pd.to_datetime(df['signup_date'])
    df['email'] = df['email'].astype(str).str.lower()  
    logging.info("Fin transformation des données")
    return df

# schéma de validation Pandera
Schema_validation = pa.DataFrameSchema({

    "user_id" : pa.Column (int, nullable= False, unique= True ),
    "age": pa.Column (int, nullable= True, unique= False, checks=[pa.Check.greater_than_or_equal_to(18),pa.Check.less_than_or_equal_to(100)] ),
    "email": pa.Column (str,  nullable= True, checks = pa.Check.str_matches(r"^((?!\.)[\w\-_.]*[^.])(@\w+)(\.\w+(\.\w+)?[^.\W])$"))
    },
    strict = False # permet d'avoir d'autres colonnes sans contraintes (ici signup_date et username)
    )

# fonction de validation
def validate_data(df) :
    logging.info("Début validation des données")

    validation_report = {
        "success": True,
        "failure_count": 0,
    }

    try : 
        df_validated = Schema_validation.validate(df, lazy=True)   # lazy = True permet de montrer toutes les erreurs (sinon ça s'arrête à la première erreur)
        logging.info("Fin validation des données - Succès.")
        return df_validated
    except pa.errors.SchemaErrors as exc:
        validation_report["success"] = False
        validation_report["failure_count"] = len(exc.failure_cases)
        failure_cases_list = exc.failure_cases.to_dict()
        validation_report["failure_details"] = failure_cases_list
        
        #print(f"\n ERREUR DE VALIDATION PANDERA : {validation_report['failure_count']} échecs détectés.\n")
        #print(f"{exc.failure_cases} \n")

    return validation_report
 

current_date = datetime.datetime.now().strftime("%Y%m%d") # calcul du timestamp actuel pour l'utiliser dans l'écriture des fichiers

# fonction d'orchestration
def run_pipeline() :
    df = extract_data()
    df_transformed = transform_data(df)

    if  validate_data(df_transformed)['failure_count'] == 0:
        df_transformed.to_parquet(rf"data\trusted\{current_date}_my_data.parquet")
        logging.info("Ajout du fichier dans le dossier 'trusted' ")

    else : 
        df_transformed.to_parquet(rf"data\quarantine\{current_date}_my_data.parquet")
        logging.info(f"Ajout du fichier dans le dossier 'quarantine' ({validate_data(df_transformed)['failure_count']} erreurs détectées) ")

        with open(rf"data\quarantine\{current_date}_my_data_report.json", 'w') as fp: # méthode pour écrire des données dans un fichier json
            json.dump(validate_data(df_transformed), fp, sort_keys = True, indent = 4 ) #indent = 4 pour formatter le json pour le rendre plus lisible ; sort_keys pour avoir les clés par ordre alphabétique
        logging.info(f"Ajout du rapport concernant le fichier dans le dossier 'quarantine' ")

    return    

#run_pipeline()

def send_alert(mail_address=None,message=None ):

    with open("demofile.txt", "a") as f:
        f.write(message)

message = """CRITICAL: ETL Pipeline failed for users_raw.csv. See quarantine for details.\n"""

send_alert(message=message)

