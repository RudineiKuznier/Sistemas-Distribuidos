import requests
from flask import Flask, request

app = Flask(__name__)
COBRANCAS = {}  

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
    status = request.get_json()["status"]
    requests.post(COBRANCAS[oid], json={"orderId": oid, "status": status}, timeout=3)
    return {"ok": True}

if __name__ == '__main__':
    app.run(port=5003)