import os
from flask import Flask, jsonify, request
from flasgger import Swagger
import resend
from dotenv import load_dotenv

resend.api_key = os.getenv("RESEND_API_KEY")

app = Flask(__name__)

# Configuração básica do Swagger UI
template = {
    "swagger": "2.0",
    "info": {
        "title": "API de Itens",
        "description": "Documentação da API REST de gerenciamento de itens.",
        "version": "1.0.0"
    }
}

swagger = Swagger(app, template=template)

# Sample data
items = [
    {"id": 1, "name": "Item 1"},
    {"id": 2, "name": "Item 2"}
]

@app.route("/")
def index():
  params = {
    "from": "onboarding@resend.dev",
    "to": "rudineisilva@alunos.utfpr.edu.br",
    "subject": "Hello World",
    "html": "<strong>Pirilampo</strong>",
  }

  r = resend.Emails.send(params)
  return jsonify(r)

@app.route('/items', methods=['GET'])
def get_items():
    """
    Listar todos os itens
    ---
    tags:
      - Itens
    responses:
      200:
        description: Lista de itens cadastrados
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: integer
                example: 1
              name:
                type: string
                example: "Item 1"
    """
    return jsonify(items), 200

@app.route('/items', methods=['POST'])
def add_item():
    """
    Criar um novo item
    ---
    tags:
      - Itens
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - name
          properties:
            name:
              type: string
              example: "Item 3"
    responses:
      201:
        description: Item criado com sucesso
        schema:
          type: object
          properties:
            id:
              type: integer
              example: 3
            name:
              type: string
              example: "Item 3"
    """
    new_item = request.get_json()
    new_item["id"] = len(items) + 1  # Assign an ID
    items.append(new_item)
    return jsonify(new_item), 201

@app.route('/items/<int:item_id>', methods=['GET'])
def get_item(item_id):
    """
    Obter um item por ID
    ---
    tags:
      - Itens
    parameters:
      - name: item_id
        in: path
        type: integer
        required: true
        description: ID do item a ser buscado
    responses:
      200:
        description: Item encontrado
        schema:
          type: object
          properties:
            id:
              type: integer
              example: 1
            name:
              type: string
              example: "Item 1"
      404:
        description: Item não encontrado
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Item not found"
    """
    item = next((item for item in items if item["id"] == item_id), None)
    if item:
        return jsonify(item), 200
    return jsonify({"error": "Item not found"}), 404

@app.route('/items/<int:item_id>', methods=['PUT'])
def update_item(item_id):
    """
    Atualizar um item por ID
    ---
    tags:
      - Itens
    parameters:
      - name: item_id
        in: path
        type: integer
        required: true
        description: ID do item a ser atualizado
      - in: body
        name: body
        required: true
        schema:
          type: object
          properties:
            name:
              type: string
              example: "Item Atualizado"
    responses:
      200:
        description: Item atualizado com sucesso
        schema:
          type: object
          properties:
            id:
              type: integer
              example: 1
            name:
              type: string
              example: "Item Atualizado"
      404:
        description: Item não encontrado
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Item not found"
    """
    updated_data = request.get_json()
    for item in items:
        if item["id"] == item_id:
            item.update(updated_data)
            return jsonify(item), 200
    return jsonify({"error": "Item not found"}), 404

@app.route('/items/<int:item_id>', methods=['DELETE'])
def delete_item(item_id):
    """
    Remover um item por ID
    ---
    tags:
      - Itens
    parameters:
      - name: item_id
        in: path
        type: integer
        required: true
        description: ID do item a ser removido
    responses:
      200:
        description: Item deletado com sucesso
        schema:
          type: object
          properties:
            message:
              type: string
              example: "Item deleted"
      404:
        description: Item não encontrado
        schema:
          type: object
          properties:
            error:
              type: string
              example: "Item not found"
    """
    global items
    item = next((item for item in items if item["id"] == item_id), None)
    if item:
        items = [item for item in items if item["id"] != item_id]
        return jsonify({"message": "Item deleted"}), 200
    else:
        return jsonify({"error": "Item not found"}), 404

if __name__ == '__main__':
    app.run(debug=True)