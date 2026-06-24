def run_pipeline():

    #-----------------------
    #Imports
    #-----------------------
    
    import os
    import pandas as pd
    import requests
    import socket
    import datetime
    import json
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    import smtplib, ssl
    import clts_pcp as clts
    import pymysql
    import unicodedata
    import io
    import re
    import unicodedata
    import numpy as np
    import warnings
    import pymongo
    import crate

    print("CWD:", os.getcwd())
    print("PEM exists:", os.path.exists(f"secrets/{user}-{db}.pem"))
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
        #Set user
        user=notebookname.split("_")[0]
    
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
    
    if ENV != "Colab":
        #Set user
        user = script.split("_")[0]
    
    print("USER:", user)
    
    ##
    if ENV == "Colab":
        clts.elapt[f"running <a href='https://colab.research.google.com/drive/{script.replace('fileId=', '')}'>google colab notebook</a>"] = clts.deltat(tstart)
    else:
        try:
            clts.elapt[f"script filename: {script}"] = clts.deltat(tstart)
            conf = Variable.get(script.replace('.py', ''), default_var={}, deserialize_json=True)
            clts.elapt[f"Params read from variable: {conf}"] = clts.deltat(tstart)
        except Exception as e:
            conf = {"status": f"error reading from {script.replace('.py', '')}"}
            clts.elapt[f"Error: {e}"] = clts.deltat(tstart)
    
        config = {**DEFAULT_PARAMS, **conf}
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
    url = "https://api.github.com/repos/pedroccpimenta/datafiles/contents/eRedes%20-%20Balcão%20Digital?ref=master"
    folders = requests.get(url, headers=headers).json()
    
    #Get every file per year
    all_files = []
    
    for fold in folders:
        if fold["type"] == "dir":
            files = requests.get(fold["url"], headers=headers).json()
            all_files.extend(files)
    #Print How Many Files There Are
    clts.elapt[f"Number of files loaded {len(all_files)}"] = clts.deltat(tstart)
    print("Number of files loaded:", len(all_files))
    
    
    warnings.filterwarnings(
    "ignore",
    message="Workbook contains no default style"
    )
    
    #-----------------------
    #Data Concatenation
    #-----------------------
    
    all_dfs = []
    
    INVALID_VALUES = {"-", "", "?", "N/A", "NA", "null", "None"}
    
    fls = all_files[0]
    
    filename = fls["name"]
    print(f"Processing: {filename}")

    url = fls["download_url"]

    # Download file
    res = requests.get(url, headers=headers)
    res.raise_for_status()

    # Read Excel directly from memory
    excel_file = io.BytesIO(res.content)

    # Find header row
    pre = pd.read_excel(
        excel_file,
        header=None,
        nrows=15
    )

    header_row = 0

    for i, row in pre.iterrows():
        row_text = " ".join(
            str(x).lower()
            for x in row
            if pd.notna(x)
        )

        if "data" in row_text and "hora" in row_text:
            header_row = i
            break

    # Need to recreate BytesIO because read_excel consumed it
    excel_file = io.BytesIO(res.content)

    df = pd.read_excel(
        excel_file,
        skiprows=header_row
    )

    # Clean column names
    df.columns = [
        re.sub(
            r"_+",
            "_",
            re.sub(
                r"[^a-zA-Z0-9_]",
                "_",
                unicodedata.normalize("NFKD", str(col))
                .encode("ascii", "ignore")
                .decode("utf-8")
                .strip()
                .lower()
            )
        ).strip("_")
        for col in df.columns
    ]

    # Remove bad columns
    df = df.loc[:, ~df.columns.str.contains("unnamed", case=False)]
    df = df.loc[:, ~df.columns.str.fullmatch(r"\d+")]
    df = df.loc[:, df.columns != ""]

    # Merge data + hora
    if "data" in df.columns and "hora" in df.columns:

        df["data"] = pd.to_datetime(
            df["data"].astype(str) + " " + df["hora"].astype(str),
            errors="coerce"
        )

        df = df.drop(columns=["hora"])
        df = df.rename(columns={"data": "timestamp"})

    # Clean invalid values
    df = df.replace(list(INVALID_VALUES), pd.NA)

    all_dfs.append(df)
    
    # Final table
    final_table = pd.concat(all_dfs, ignore_index=True)
    
    # Final cleanup
    final_table["timestamp"] = pd.to_datetime(
        final_table["timestamp"],
        errors="coerce"
    )
    
    final_table = final_table.dropna(subset=["timestamp"])
    
    final_table = final_table.drop_duplicates(
        subset=["timestamp"],
        keep="last"
    )
    
    final_table = final_table.reset_index(drop=True)
    
    print(f"Rows: {len(final_table)}")
    print(f"Columns: {len(final_table.columns)}")
    print(f"Total NaNs: {final_table.isna().sum().sum()}")
    
    
    warnings.filterwarnings(
        "ignore",
        category=FutureWarning
    )
    
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
    
    # Add missing columns to TiDB
    def sync_tidb_columns(cursor, table_name, df):
    
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    
        existing_cols = {
            row["Field"]
            for row in cursor.fetchall()
        }
    
        for col in df.columns:
    
            if col not in existing_cols:
    
                col_type = map_dtype(df[col].dtype)
    
                sql = f"""
                ALTER TABLE `{table_name}`
                ADD COLUMN `{col}` {col_type}
                """
    
                cursor.execute(sql)
    
                print(f"Added column to TiDB: {col}")
    
    
    # Add missing columns to CrateDB
    def sync_crate_columns(cursor, table_name, df):
    
        cursor.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """)
    
        existing_cols = {
            row[0]
            for row in cursor.fetchall()
        }
    
        for col in df.columns:
    
            if col not in existing_cols:
    
                col_type = map_dtype(df[col].dtype)
    
                if col_type == "DATETIME":
                    col_type = "TIMESTAMP"
    
                cursor.execute(
                    f'ALTER TABLE "{table_name}" '
                    f'ADD COLUMN "{col}" {col_type}'
                )
    
                print(f"Added column to CrateDB: {col}")
    
    
    ##
    #Start Connection
    clts.elapt[f"Starting database accesses:"] = clts.deltat(tstart)
    
    #List of databases
    dblist=json.loads(get_secret(f"{user}-dblist.json"))
    print(dblist)
    
    db_stats = {}
    #Iterate per database
    for db in dblist:
    
        #Stats
        db_insert_start = datetime.datetime.now()
    
        db_stats[db] = {
            "rows": 0,
            "bytes": 0,
            "seconds": 0
        }
        #Connection
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
    
                pem_content = userdata.get(dbcreds['pem'])
                with open(f'/tmp/{user}.pem', 'w') as f:
                    f.write(pem_content)
                pem_path = f"/tmp/{user}.pem"

    
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
    
                #Create table if not yet
                sql = """
                CREATE TABLE IF NOT EXISTS energia (
                    `timestamp` TIMESTAMP PRIMARY KEY
                )
                """
                cursor.execute(sql)
    
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
    
                #Create table if not yet
                sql = """
                CREATE TABLE IF NOT EXISTS energia (
                    "timestamp" TIMESTAMP PRIMARY KEY
                )
                """
                cursor.execute(sql)
    
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
    
        ### INSERTION OF DATA ###
    
        if status == "ok":
    
            table_name = "energia"
    
            # CREATE TABLE + ADD NEW COLUMNS IF NEEDED
            if dbcreds["dbms"] == "sql_tls":
    
                sync_tidb_columns(cursor, table_name, final_table)
                connection.commit()
    
            elif dbcreds["dbms"] == "crate":
    
                sync_crate_columns(cursor, table_name, final_table)
                connection.commit()
    
            #EXTRA CLEANING
            # Check if empty
            if final_table.empty:
                continue
    
            inserts = len(final_table)
    
            columns = final_table.columns.tolist()
            col_names = ", ".join(columns)
    
            # Remove inf values
            final_table = final_table.replace([np.inf, -np.inf], np.nan)
    
            # Convert NaN -> None
            final_table = final_table.astype(object).where(pd.notna(final_table), None)
    
            values = list(final_table.itertuples(index=False, name=None))
            key_column = "timestamp"
    
            # TiDB
            if dbcreds["dbms"] == "sql_tls":
    
                placeholders = ", ".join(["%s"] * len(columns))
    
                sql = f"""
                INSERT IGNORE INTO {table_name} ({col_names})
                VALUES ({placeholders})
                """
                cursor.executemany(sql, values)
                connection.commit()
    
            # CrateDB
            elif dbcreds["dbms"] == "crate":
    
                placeholders = ", ".join(["?"] * len(columns))
    
                sql = f"""
                INSERT INTO {table_name} ({col_names})
                VALUES ({placeholders})
                ON CONFLICT ({key_column}) DO NOTHING
                """
    
                cursor.executemany(sql, values)
                connection.commit()
    
            # MongoDB
            elif dbcreds["dbms"] == "mongodb":
                from pymongo.errors import BulkWriteError
    
                database = connection[dbcreds["database"]]
                collection = database[table_name]
    
                collection.create_index([("timestamp", 1)], unique=True)
    
                records = final_table.to_dict("records")
    
                CHUNK_SIZE = 1000
    
                try:
                  for i in range(0, len(records), CHUNK_SIZE):
                        chunk = records[i:i + CHUNK_SIZE]
    
                        collection.insert_many(chunk, ordered=False)
    
                except BulkWriteError:
                      pass
    
            print(f"... {inserts} rows inserted into {table_name} for {db}")
            clts.elapt[f"... {inserts} rows inserted into {table_name} for {db}"] = clts.deltat(tstart)
    
    #-----------------------
    #Email
    #-----------------------
    
    clts.elapt["Overall (before email):"] = clts.deltat(tstart)
    
    if send_mail and email_addresses:
    
        import datetime
    
        total_rows = inserts
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
        
    
