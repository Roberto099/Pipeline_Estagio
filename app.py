from flask import Flask
from RP_eRedes import run_pipeline
import os

app = Flask(__name__)

@app.route("/")
def home():
    result = run_pipeline()
    return {"status": result}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
