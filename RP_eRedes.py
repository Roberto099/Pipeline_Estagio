#-----------------------
#Imports
#-----------------------

import os
import pandas as pd
import requests
import socket
import datetime
import json
import crate
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib, ssl
import clts_pcp as clts
import pymysql
import pymongo
from urllib.parse import quote
import tempfile

def run_pipeline():

    #-----------------------
    #Context Gathering
    #-----------------------

    #Enviromnent Identification
    ENV = os.getenv("APP_ENV")

    #Start timer
    tstart=clts.getts()

    #Default configuration
    DEFAULT_PARAMS = {
        "verbose": True,
        "destination": "-*-",
        "send_mail": True,
        "email_addresses": ["granderoberto7e@gmail.com"]
    }

    #Get hostname and machine IP and print
    hostname=socket.gethostname()

    try:
        ip = requests.get("https://api.ipify.org", timeout=5).text
    except Exception:
        ip = "0.0.0.0"

    print("Server name:", hostname, "Public IP Address:", ip)

    #Path Handling
    base_dir = os.getenv("BASE_DIR", os.getcwd())

    #Create safe path
    datapath = os.path.join(base_dir, "data")
    print("datapath:", datapath)

    ##Identify environment

    if ENV is None:

        #Check Colab
        try:
            import google.colab
            ENV = "colab"
        except ImportError:

            #Check Jupyter
            try:
                from IPython import get_ipython
                if get_ipython() is not None:
                    ENV = "jupyter"
                else:
                    ENV = "Flask"
            except Exception:
                ENV = "Flask"
        
    #Check Render/Flask
    if os.getenv("RENDER"):
        ENV = "Render"
        from airflow.models import Variable
        
    print("Detected ENV:", ENV)


    #Specifications based on the ENV
    if ENV == "colab":
        print("Running in Colab")
        #COLAB imports
        from google.colab import userdata
        import ipynbname
        #Folder where notebook is located
        folder_path = os.getcwd()
        print("folder_path:", folder_path)
        notebookname = requests.get("http://172.28.0.12:9000/api/sessions").json()[0]["name"]
        print("Notebook:", notebookname)
        #Identify user
        user=notebookname.split("-")[0]
        print ("user:", user)

        #Set variables
        parts=[hostname, user, "eRedes data" , ipynbname.name()]
        datapath="."
        destination=DEFAULT_PARAMS['destination']
        verbose= DEFAULT_PARAMS['verbose']
        send_mail = DEFAULT_PARAMS['send_mail']
        email_addresses = DEFAULT_PARAMS['email_addresses']
        
    elif ENV == "Render":
        print("Running in Render")
        script_path = os.path.abspath(__file__)
        parts = __file__.replace('\\', "/").split('/')
        datapath=f'./data/ppimenta/{parts[-1]}'
    
    elif ENV == "Flask":
        print("Running local with flask")
        script_path = globals().get("__file__", "app.py")
        print(script_path)
        parts = script_path.replace("\\", "/").split("/")

    #Info
    script = parts[-1]
    channel = parts[-2]
    user = script.split("_")[0]

    ##
    if ENV == "colab.google":
        clts.elapt[f"running <a href='https://colab.research.google.com/drive/{script.replace('fileId=', '')}'>google colab notebook</a>"] = clts.deltat(tstart)
    else:
        try:
            clts.elapt[f"script filename: {script}"] = clts.deltat(tstart)
            airflow_conf = Variable.get(script.replace('.py', ''), default_var={}, deserialize_json=True)
            clts.elapt[f"Params read from variable: {airflow_conf}"] = clts.deltat(tstart)
        except Exception as e:
            airflow_conf = {"status": f"error reading from {script.replace('.py', '')}"}
            clts.elapt[f"Error: {e}"] = clts.deltat(tstart)

        config = {**DEFAULT_PARAMS, **airflow_conf}
        verbose = config['verbose']
        destination = config['destination']
        send_mail = config['send_mail']
        email_addresses = config['email_addresses']

    context = f'{hostname} ({ip}) | {user} | {channel} | {script} | {destination}'
    clts.setcontext(context)
    now = str(datetime.datetime.now())[0:19]
    hoje = now[:10]

    if verbose:
        print("context:", context)

    #-----------------------
    #Secrets Definition
    #-----------------------

    if ENV == "colab":
        def get_secret(secret):
            return userdata.get(secret)
    
    else:
        def get_secret(secret):
            path = f"secrets/{secret}"

            with open(path, "r") as f:
                return f.read()


    #-----------------------
    #Connection With Github
    #-----------------------

    Token = json.loads(get_secret(f"{user}-github_token.json"))["key"]
    headers = {"Authorization": f"token {Token}"}
    url = "https://api.github.com/repos/pedroccpimenta/datafiles/contents/eRedes%20-%20Balcão%20Digital/2026?ref=master"
    response = requests.get(url, headers=headers)
    files = response.json()
    #Print How Many Files There Are
    clts.elapt[f"Number of files loaded {len(files)}"] = clts.deltat(tstart)
    print("Number of files loaded:", len(files))


    #-----------------------
    #Data Dowload
    #-----------------------

    os.makedirs("data", exist_ok=True)

    skip = 0

    for fls in files:
        skip += 1
        if skip == 3:
            break
        filename = fls['name']
        print(filename)
        #Make url format
        url_form = filename.replace(' ', '%20')
        url = f"https://raw.githubusercontent.com/pedroccpimenta/datafiles/master/eRedes%20-%20Balc%C3%A3o%20Digital/2026/{url_form}"

        res = requests.get(url, headers=headers)

        #Transfer the files to colab
        with open(f"data/{filename}", "wb") as f:
            f.write(res.content)

    clts.elapt[f"Data loaded to local enviromnet"] = clts.deltat(tstart)

    #-----------------------
    #Data Concatenation
    #-----------------------
    
    dfs = []

    #Make List
    for file in os.listdir("data"):
        if file.endswith(".xlsx"):
            #Skip header junk
            df = pd.read_excel(f"data/{file}", skiprows=9)
            #Clean column names
            df.columns =(
            df.columns
            .str.strip()
            .str.replace(" ", "_")
            )
            #append
            dfs.append(df)
    
    #Concat
    final_df = pd.concat(dfs, ignore_index=True)

    #Make "Data" and "Hora" be the same Column
    final_df["Data"] = pd.to_datetime(
        final_df["Data"] + " " + final_df["Hora"],
        errors="coerce"
    )

    #Drop "Hora"
    final_df = final_df.drop(columns=["Hora"])

    #Simplify Columns Names
    final_df = final_df.rename(columns={
        "Data": "timestamp",
        "Potência_Ativa_Saldo_(kW)_-_Consumo": "potencia_ativa",
        "Potência_Reativa_Indutiva_(kVAr)_-_Consumo": "potencia_reativa_indutiva",
        "Potência_Reativa_Capacitiva_(kVAr)_-_Consumo": "potencia_reativa_capacitiva"
    })

    clts.elapt[f"Concatenation completed"] = clts.deltat(tstart)
    
    #See results
    print(final_df.columns)
    print(final_df.shape)
    print(final_df.dtypes)
    final_df.head()


    #-----------------------
    #Connection with databases and insertion of data
    #-----------------------

    clts.elapt[f"Starting database accesses:"] = clts.deltat(tstart)

    #List of databases
    dblist=json.loads(get_secret(f"{user}-dblist.json"))
    print(dblist)

    rows_per_db = {}
    size_per_db = {}

    #Iterate per database
    for db in dblist:
        status="nok"
        clts.elapt[f"Connecting to `{db}`"] = clts.deltat(tstart)
        if verbose:
            print ("db in dblist:", db)
            print (f'connecting to `{db}`')
        try:
            print (f"Credentials in `{user}-{db}.json`")
            dbcreds=json.loads(get_secret(f"{user}-{db}.json"))

            #TiDB
            if dbcreds['dbms']=="sql_tls":
                print("... connecting to sql_tls database...")
                timeout = dbcreds['timeout']

                if ENV == "colab":
                    pem_content = userdata.get(dbcreds['pem'])
                    with open(f'/tmp/{user}.pem', 'w') as f:
                        f.write(pem_content)
                    pem_path = f"/tmp/{user}.pem"
                
                else:
                    pem_path = f"secrets/{user}-{db}.pem"

                connection = pymysql.connect(
                    host=dbcreds["dest_host"],
                    port=dbcreds["port"],
                    db=dbcreds['database'],
                    user=dbcreds['username'],
                    password=dbcreds['password'],
                    cursorclass=pymysql.cursors.DictCursor,
                    charset="utf8mb4",
                    ssl={'ca': pem_path},
                    connect_timeout=timeout,
                    write_timeout=timeout,
                    read_timeout=timeout,
                    autocommit=True
                )
                cursor = connection.cursor()
                clts.elapt[f"... connected to `{db}`"] = clts.deltat(tstart)
                status = "ok"

            #Crate
            elif dbcreds['dbms']=="crate":
                print("... connecting to crate database...")
                from crate import client
                connection = client.connect(
                    dbcreds["dest_host"],
                    username=dbcreds["username"],
                    password=dbcreds["password"],
                    verify_ssl_cert=True
                )
                cursor = connection.cursor()
                clts.elapt[f"... connected to `{db}`"] = clts.deltat(tstart)
                status = "ok"
            
            #MongoDB
            elif dbcreds['dbms'] == "mongodb":
                print("... connecting to mongodb database...")
                from pymongo import MongoClient

                timeout = dbcreds.get("timeout", 10000)

                connection = MongoClient(host=dbcreds["uri"])

                clts.elapt[f"... connected to `{db}`"] = clts.deltat(tstart)
                status = "ok"
        
        #Error
        except Exception as e:
            print("Error:", e)
            clts.elapt[f"... error `{e}` ❌"] = clts.deltat(tstart)
            status='onerror'


        ###INSERTION OF DATA###

        if status == "ok":
            #Count inserts
            inserts = 0

            #TiDB insertion
            if dbcreds['dbms'] == "sql_tls":
                sql = """
                INSERT INTO energia (
                timestamp,
                potencia_ativa,
                potencia_reativa_indutiva,
                potencia_reativa_capacitiva
                ) VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                potencia_ativa = VALUES(potencia_ativa),
                potencia_reativa_indutiva = VALUES(potencia_reativa_indutiva),
                potencia_reativa_capacitiva = VALUES(potencia_reativa_capacitiva)
                """

                values = [
                (
                    row["timestamp"],
                    row["potencia_ativa"],
                    row["potencia_reativa_indutiva"],
                    row["potencia_reativa_capacitiva"]
                )
                for _, row in final_df.iterrows()
                ]

                cursor.executemany(sql, values)
                connection.commit()
                inserts += len(values)

                cursor.execute("""
                    SELECT data_length + index_length
                    FROM information_schema.TABLES
                    WHERE table_schema = %s
                    AND table_name = 'energia'
                """, (dbcreds["database"],))

                row = cursor.fetchone()
                size_per_db[db] = list(row.values())[0] if row else 0

            #Crate insertion
            elif dbcreds['dbms'] == "crate":
                sql = """
                INSERT INTO energia (
                timestamp,
                potencia_ativa,
                potencia_reativa_indutiva,
                potencia_reativa_capacitiva
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT (timestamp) DO UPDATE SET
                potencia_ativa = excluded.potencia_ativa,
                potencia_reativa_indutiva = excluded.potencia_reativa_indutiva,
                potencia_reativa_capacitiva = excluded.potencia_reativa_capacitiva
                """

                values = [
                (
                    row["timestamp"],
                    row["potencia_ativa"],
                    row["potencia_reativa_indutiva"],
                    row["potencia_reativa_capacitiva"]
                )
                for _, row in final_df.iterrows()
                ]

                cursor.executemany(sql, values)
                connection.commit()
                inserts += len(values)

                cursor.execute("""
                    SELECT sum(size)
                    FROM sys.shards
                    WHERE table_name = 'energia'
                """)

                row = cursor.fetchone()
                size_per_db[db] = row[0] if row else 0

            #MongoDB insertion
            elif dbcreds['dbms'] == "mongodb":

                database = connection[dbcreds["database"]]
                collection = database["energia"]

                from pymongo import UpdateOne

                ops = [
                    UpdateOne(
                        {"timestamp": row["timestamp"]},
                        {"$set": {
                            "potencia_ativa": row["potencia_ativa"],
                            "potencia_reativa_indutiva": row["potencia_reativa_indutiva"],
                            "potencia_reativa_capacitiva": row["potencia_reativa_capacitiva"]
                        }},
                        upsert=True
                    )
                    for _, row in final_df.iterrows()
                ]

                if ops:
                    collection.bulk_write(ops)

                inserts = len(ops)

                stats = database.command("collStats", "energia")
                size_per_db[db] = stats["size"]
            
            #Results of insertions
            clts.elapt[f"... {inserts} rows inserted, for {db}"] = clts.deltat(tstart)
            print(f"... {inserts} rows inserted, for {db}")
            rows_per_db[db] = inserts

    #-----------------------
    #Email
    #-----------------------

    clts.elapt["Overall (before email):"] = clts.deltat(tstart)

    if send_mail and email_addresses != []:

        # resumo
        total_rows = inserts
        dbs = ", ".join(dblist)
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        toem = f"""
        Energia - sincronização concluída

        Data: {now}
        Bases de dados: {dbs}
        Registos enviados: {total_rows}

        {clts.listtimes()}
        """
    
    credsgmail = json.loads(get_secret(f"configGMail_{user}.json"))


    try:
      assunto = f"⚡ Energia sync — {total_rows} rows"

      message = MIMEMultipart("alternative")
      message["Subject"] = assunto
      message["From"] = credsgmail['UserFrom']
      message["To"] = ", ".join(email_addresses)
      message["Reply-To"]="granderoberto7e@gmail.com"

      text = toem

      html = f"""
      <html>
      <body style="font-family:Montserrat;">
      <h3>⚡ Energia — sincronização</h3>

      <table border="1" cellpadding="6" cellspacing="0">
      <tr><td><b>Data</b></td><td>{now}</td></tr>
      <tr><td><b>Databases</b></td><td>{dbs}</td></tr>
      <tr><td><b>Rows</b></td><td>{total_rows}</td></tr>
      </table>

      <br>
      <pre>{clts.listtimes()}</pre>

      <hr>
      Automated energy ingestion pipeline
      </body>
      </html>
      """

      message.attach(MIMEText(text, "plain"))
      message.attach(MIMEText(html, "html"))

      port = 465
      ssl_context = ssl.create_default_context()

      with smtplib.SMTP_SSL("smtp.gmail.com", port, context=ssl_context) as server:
          server.login(credsgmail['UserName'], credsgmail['UserPwd'])
          server.sendmail(
              credsgmail['UserFrom'],
              email_addresses,
              message.as_string()
          )

      print("Nottification sended")

    except Exception as e:
        print("Erro email:", e)
    