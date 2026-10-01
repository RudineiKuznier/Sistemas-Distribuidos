from flask import Flask
from flasgger import Swagger

app = Flask(__name__)
swagger = Swagger(app)

@app.route('/')
def home():
    """
    Rota inicial de boas-vindas
    ---
    responses:
      404:
        description: Retorna a mensagem de boas-vindas
    """
    return "Olá, Flask!"

if __name__ == '__main__':
    app.run(debug=True)