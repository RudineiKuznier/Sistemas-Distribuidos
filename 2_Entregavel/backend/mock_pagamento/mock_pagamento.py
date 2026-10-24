
import os
import requests
from flask import Flask, request
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
COBRANCAS = {}  
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

PAGINA = """
<h2>Pagamento simulado - Pedido {oid}</h2>
<button onclick="enviar('APROVADO')">Pagamento Aprovado</button>
<button onclick="enviar('RECUSADO')">Pagamento Recusado</button>
<p id="msg"></p>
<script>
async function enviar(status) {
  const r = await fetch('/checkout/{oid}/result', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({status})
  });
  document.getElementById('msg').textContent = r.ok ? 'Enviado: ' + status : 'Erro';
}
</script>
"""

@app.post('/charges')
def criar_cobranca():
    d = request.get_json()
    COBRANCAS[d["orderId"]] = d["webhookUrl"]
    return {"checkoutUrl": f"http://localhost:5003/checkout/{d['orderId']}"}, 201

@app.get('/checkout/<oid>')
def checkout(oid):
    return PAGINA.replace("{oid}", oid)

@app.post('/checkout/<oid>/result')
def resultado(oid):
    webhook_url = COBRANCAS.get(oid)
    if not webhook_url:
        return {"error": "cobrança não encontrada"}, 404

    status = (request.get_json() or {}).get("status")
    if status not in ("APROVADO", "RECUSADO"):
        return {"error": "status inválido"}, 400

    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Token": WEBHOOK_SECRET,
    }

    try:
        resposta = requests.post(
            webhook_url,
            json={"orderId": oid, "status": status},
            headers=headers,
            timeout=3,
        )
        resposta.raise_for_status()
    except requests.RequestException as e:
        print(f"[Webhook erro] {e}")
        return {"error": "falha ao chamar o webhook"}, 502

    return {"ok": True}, 200

if __name__ == '__main__':
    app.run(port=5003)