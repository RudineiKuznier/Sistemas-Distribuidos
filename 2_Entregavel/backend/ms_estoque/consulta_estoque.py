import csv
import os
from flask import Flask

CAMINHO_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "database.csv")
app = Flask(__name__)


@app.route('/products')
def listar_produtos():
    with open(CAMINHO_CSV, encoding="utf-8-sig", newline="") as f:
        return [
            {**l, "price": float(l["price"]), "stock": int(l["stock"])}
            for l in csv.DictReader(f)
            #if int(l["stock"]) > 0 #validação no front 
        ]


if __name__ == '__main__':
    app.run(port=5001)