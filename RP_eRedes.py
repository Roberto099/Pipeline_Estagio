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
import unicodedata
import re
import hashlib
import unicodedata
import numpy as np
from pymongo.errors import BulkWriteError

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
            ENV = "Colab"
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
        
    #Check Render
    if os.getenv("RENDER"):
        ENV = "Render"
        
    print("Detected ENV:", ENV)


    #Specifications based on the ENV
    if ENV == "Colab":
        print("Running in Colab")
        #COLAB imports
        from google.colab import userdata
        import ipynbname
        #Folder where notebook is located
        folder_path = os.getcwd()
        print("folder_path:", folder_path)
        notebookname = requests.get("http://172.28.0.12:9000/api/sessions").json()[0]["name"]
        print("Notebook:", notebookname)

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
    
    elif ENV == "Flask":
        print("Running local with flask")
        script_path = globals().get("__file__", "app.py")
        print(script_path)
        parts = script_path.replace("\\", "/").split("/")

    #Info
    script = parts[-1]
    channel = parts[-2]

    if ENV == "Colab":
        #Set user
        user=notebookname.split("_")[0]
        print ("user:", user)
    else:    
        #Set user
        user = script.split("_")[0]
    
    print("USER:", user)

    ##
    if ENV == "Colab":
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

    if verbose:
        print("context:", context)

    #-----------------------
    #Secrets Definition
    #-----------------------

    if ENV == "Colab":
        def get_secret(secret):
            return userdata.get(secret)
    
    elif ENV == "Render":
        def get_secret(secret):
            path = f"/etc/secrets/{secret}"

            with open(path, "r") as f:
                return f.read()
    
    elif ENV == "Flask":
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

    for fls in files:
        filename = fls['name']
        print(filename)
        #Make url format
        url_form = filename.replace(' ', '%20')
        url = f"https://raw.githubusercontent.com/pedroccpimenta/datafiles/master/eRedes%20-%20Balc%C3%A3o%20Digital/2026/{url_form}"

        res = requests.get(url, headers=headers)

        #Transfer the files to colab
        with open(f"data/{filename}", "wb") as f:
            f.write(res.content)
        break

    clts.elapt[f"Data loaded to local enviromnet"] = clts.deltat(tstart)

    #-----------------------
    #Data Concatenation
    #-----------------------
    
    import warnings

    warnings.filterwarnings(
        "ignore",
        message="Workbook contains no default style"
    )

    #-----------------------
    #Data Concatenation
    #-----------------------

    # Stores grouped tables
    tables = {}

    #Make List
    for file in os.listdir("data"):
        if file.endswith(".xlsx"):
            #FilePath
            filepath = os.path.join("data", file)
            #Read first 15 rows
            pre = pd.read_excel(filepath, header=None, nrows=15)
            header_row = 0

            for i, row in pre.iterrows():
                row_text = " ".join(
                    str(x).strip().lower()
                    for x in row
                    if pd.notna(x)
                )

                if "data" in row_text and "hora" in row_text:
                    header_row = i
                    break

            df = pd.read_excel(filepath, skiprows=header_row)

            # Clean column names
            df.columns = [
                re.sub(r"_+", "_",
                    re.sub(r"[^a-zA-Z0-9_]", "_",
                        unicodedata.normalize("NFKD", str(col))
                        .encode("ascii", "ignore")
                        .decode("utf-8")
                        .strip()
                        .lower()
                    )
                ).strip("_")
                for col in df.columns
            ]

            #Join data and hora
            if "data" in df.columns and "hora" in df.columns:

                df["data"] = pd.to_datetime(
                    df["data"].astype(str) + " " + df["hora"].astype(str),
                    errors="coerce"
                )

                df = df.drop(columns=["hora"])

                df = df.rename(columns={"data": "timestamp"})

            # tuple makes it hashable
            schema = tuple(df.columns)

            # Create deterministic schema hash
            schema_hash = hashlib.md5(
                str(schema).encode()
            ).hexdigest()[:8]

            table_name = f"energia_{schema_hash}"

            #Check if its new in this run
            if table_name not in tables:

                print(f"NEW TABLE STRUCTURE FOUND: {table_name}")

                print(schema)

                tables[table_name] = []

            # Append dataframe
            tables[table_name].append(df)


    ##
    INVALID_VALUES = {"-", "", "?", "N/A", "NA", "null", "None"}

    #Loop trough schemas and clean them
    for table_name in tables:

        cleaned_dfs = []

        for df in tables[table_name]:

            # Clean invalid values
            df = df.replace(list(INVALID_VALUES), pd.NA)

            cleaned_dfs.append(df)

        tables[table_name] = cleaned_dfs


    #Final concatenation
    final_tables = {}

    for table_name, dfs in tables.items():
        final_tables[table_name] = pd.concat(dfs, ignore_index=True)


    #Just checking
    for name, df in final_tables.items():

        nan_count = df.isna().sum().sum()

        print(f"{name} → total NaNs: {nan_count}")


    #-----------------------
    #Connection with databases and insertion of data
    #-----------------------

    #Define Types
    def map_dtype(dtype):
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"

        elif pd.api.types.is_float_dtype(dtype):
            return "DOUBLE"

        elif pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"

        else:
            return "TEXT"

    #Define Tables for TiDB
    def create_table_tidb(table_name, df):

        cols_sql = []

        for col in df.columns:
            col_type = map_dtype(df[col].dtype)
            cols_sql.append(f"`{col}` {col_type}")

        cols_sql = ", ".join(cols_sql)

        sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {cols_sql}
        )
        """

        return sql

    #Define Tables for crate
    def create_table_crate(table_name, df):

        cols_sql = []

        for col in df.columns:

            col_type = map_dtype(df[col].dtype)

            # Crate prefers TIMESTAMP instead of DATETIME
            if col_type == "DATETIME":
                col_type = "TIMESTAMP"

            cols_sql.append(f'"{col}" {col_type}')

        cols_sql = ", ".join(cols_sql)

        sql = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            {cols_sql}
        )
        """

        return sql


    #Start Connection
    clts.elapt[f"Starting database accesses:"] = clts.deltat(tstart)

    #List of databases
    dblist=json.loads(get_secret(f"{user}-dblist.json"))
    print(dblist)


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

                if ENV == "Colab":
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
            total_inserts = 0
            for table_name, df in final_tables.items():

                #CREATE TABLE
                if dbcreds["dbms"] == "sql_tls":

                    create_sql = create_table_tidb(table_name, df)

                    cursor.execute(create_sql)
                    connection.commit()

                elif dbcreds["dbms"] == "crate":

                    create_sql = create_table_crate(table_name, df)

                    cursor.execute(create_sql)
                    connection.commit()

                #Check if its empty
                if df.empty:
                    continue

                inserts = len(df)
                total_inserts += inserts

                # detect columns dynamically
                columns = df.columns.tolist()

                #Prepare values for insertion
                col_names = ", ".join(columns)

                clean_df = df.copy()

                # remove inf
                clean_df = clean_df.replace([np.inf, -np.inf], np.nan)

                # force full object conversion + strict None conversion
                clean_df = clean_df.astype(object).where(pd.notna(clean_df), None)

                values = list(clean_df.itertuples(index=False, name=None))

                #Make sure key is right
                key_column = "timestamp" if "timestamp" in columns else columns[0]

                #INSERT into TiDB
                if dbcreds["dbms"] == "sql_tls":

                    placeholders = ", ".join(["%s"] * len(columns))

                    sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE
                    {", ".join([f"{c}=VALUES({c})" for c in columns if c != key_column])}
                    """

                    cursor.executemany(sql, values)
                    connection.commit()

                #INSERT into crateDB
                elif dbcreds["dbms"] == "crate":

                    placeholders = ", ".join(["?"] * len(columns))

                    sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    VALUES ({placeholders})
                    ON CONFLICT ({"_id"}) DO NOTHING
                    """

                    cursor.executemany(sql, values)
                    connection.commit()

                #INSERT into MongoDB
                elif dbcreds["dbms"] == "mongodb":

                    database = connection[dbcreds["database"]]
                    collection = database[table_name]

                    # Skip empty dataframe
                    if df.empty:
                        continue

                    # Replace NaN with None
                    clean_df = df.where(pd.notnull(df), None)

                    # Convert dataframe to list of dictionaries
                    records = clean_df.to_dict("records")

                    # Insert all rows directly
                    try:
                        collection.insert_many(records, ordered=False)

                    except BulkWriteError:
                        # Ignore duplicate key errors
                        pass
                #Results of insertions for consumo
                clts.elapt[f"... {inserts} rows from {table_name} inserted, for {db}"] = clts.deltat(tstart)
                print(f"... {inserts} rows from {table_name} inserted, for {db}")

    #-----------------------
    #Email
    #-----------------------

    clts.elapt["Overall (before email):"] = clts.deltat(tstart)

    if send_mail and email_addresses:

        import datetime

        total_rows = total_inserts
        dbs = ", ".join(dblist)
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        toem = f"""
    Energia - sincronização concluída

    Data: {now}
    Bases de dados: {dbs}
    Registos processados: {total_rows}

    {clts.listtimes()}
    """

        try:
            credsgmail = json.loads(get_secret(f"configGMail_{user}.json"))

            assunto = f"⚡ Energia sync — {total_rows} rows"

            message = MIMEMultipart("alternative")
            message["Subject"] = assunto
            message["From"] = credsgmail["UserFrom"]
            message["To"] = ", ".join(email_addresses)
            message["Reply-To"] = credsgmail["UserFrom"]

            html = f"""
            <html>
            <body style="font-family:Arial;">
            <h3>⚡ Energia — sincronização</h3>

            <table border="1" cellpadding="6" cellspacing="0">
                <tr><td><b>Data</b></td><td>{now}</td></tr>
                <tr><td><b>Databases</b></td><td>{dbs}</td></tr>
                <tr><td><b>Rows</b></td><td>{total_rows}</td></tr>
            </table>

            <br>
            <pre>{clts.listtimes()}</pre>

            <hr>
            Automated pipeline
            </body>
            </html>
            """

            message.attach(MIMEText(toem, "plain"))
            message.attach(MIMEText(html, "html"))

            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as server:
                server.login(credsgmail["UserName"], credsgmail["UserPwd"])
                server.sendmail(
                    credsgmail["UserFrom"],
                    email_addresses,
                    message.as_string()
                )

            print("Notification sent")

        except Exception as e:
            print("Erro email:", e)
    
