from flask import Flask
from RP_eRedes import run_pipeline

app = Flask(__name__)

@app.route("/")
def home():
    result = run_pipeline()
    return {"status": result}


if __name__ == "__main__":
    app.run(debug=True)