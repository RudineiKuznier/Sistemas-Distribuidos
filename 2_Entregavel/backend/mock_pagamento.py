import threading
from urllib.parse import urlparse
from uuid import uuid4

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
COBRANCAS = {}
COBRANCAS_LOCK = threading.Lock()

def webhook_valido(order_id, webhook_url):
    if not isinstance(order_id, str) or not isinstance(webhook_url, str):
        return False
    try:
        parsed = urlparse(webhook_url)
        port = parsed.port
    except ValueError:
        return False

    caminho_esperado = f"/api/payments/{order_id}/webhook"
    return (
        parsed.scheme == "http"
        and parsed.hostname in ("localhost", "127.0.0.1")
        and port == 5002
        and parsed.path == caminho_esperado
        and bool(parsed.query)
    )


@app.route("/api/charges", methods=["POST"])
def criar_cobranca():
    dados = request.get_json() or {}
    order_id = dados.get("order_id")
    webhook_url = dados.get("webhook_url")

    if not order_id or not webhook_url or not webhook_valido(order_id, webhook_url):
        return jsonify({"error": "Pedido ou URL de webhook inválida."}), 400

    with COBRANCAS_LOCK:
        cobranca = next(
            (item for item in COBRANCAS.values() if item["order_id"] == order_id),
            None
        )
        if cobranca is None:
            charge_id = str(uuid4())
            cobranca = {
                "order_id": order_id,
                "webhook_url": webhook_url,
                "status": "PENDENTE"
            }
            COBRANCAS[charge_id] = cobranca
        else:
            charge_id = next(
                key for key, item in COBRANCAS.items() if item is cobranca
            )
            if cobranca["status"] == "PENDENTE":
                cobranca["webhook_url"] = webhook_url

        return jsonify({
            "charge_id": charge_id,
            "checkout_url": (
                f"http://localhost:5173/pagamento"
                f"?orderId={order_id}&chargeId={charge_id}"
            )
        }), 200


@app.route("/checkout/<charge_id>/result", methods=["POST"])
def enviar_resultado_webhook(charge_id):
    resultado = (request.get_json() or {}).get("result")
    if resultado not in ("APROVADO", "RECUSADO"):
        return jsonify({"error": "Resultado deve ser APROVADO ou RECUSADO."}), 400

    with COBRANCAS_LOCK:
        cobranca = COBRANCAS.get(charge_id)
        if cobranca is None:
            return jsonify({"error": "Cobrança não encontrada."}), 404
        if cobranca["status"] == resultado:
            return jsonify({"success": True, "duplicate": True}), 200
        if cobranca["status"] != "PENDENTE":
            return jsonify({"error": "Cobrança já finalizada com outro resultado."}), 409

        try:
            response = requests.post(
                cobranca["webhook_url"],
                json={"result": resultado},
                timeout=5
            )
        except requests.RequestException as erro:
            print(f"[Payment Webhook Error] {erro}")
            return jsonify({"error": "Não foi possível contatar o MS Pagamento."}), 502

        try:
            resposta = response.json()
        except ValueError:
            resposta = {"error": "Resposta inválida do MS Pagamento."}
        if not response.ok:
            return jsonify(resposta), response.status_code

        cobranca["status"] = resultado
        return jsonify({"success": True}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003, debug=False, use_reloader=False)
