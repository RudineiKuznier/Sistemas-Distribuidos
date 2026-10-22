import json
import base64
import os
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes, serialization

def assinar_evento(payload, nome_produtor, caminho_chave_privada):
    with open(caminho_chave_privada, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    
    dados_para_assinar = {"produtor": nome_produtor, "payload": payload}
    dados_bytes = json.dumps(dados_para_assinar, sort_keys=True).encode('utf-8')
    raw_signature = private_key.sign(dados_bytes, padding.PKCS1v15(), hashes.SHA256())

    return {
        "produtor": nome_produtor, 
        "Signature": base64.b64encode(raw_signature).decode('utf-8'),
        "payload": payload
    }

def verificar_evento(envelope, pasta_chaves="chaves_publicas"):
    try:
        produtor = envelope.get("produtor")
        if not produtor:
            print("     Envelope sem identificação de produtor.")
            return False

        caminho_chave_publica = os.path.join(pasta_chaves, f"{produtor}.pub")
        if not os.path.exists(caminho_chave_publica):
            print(f"    Chave pública para '{produtor}' não encontrada em {caminho_chave_publica}")
            return False

        with open(caminho_chave_publica, "rb") as f:
            public_key = serialization.load_pem_public_key(f.read())
        
        signature_bytes = base64.b64decode(envelope["Signature"])
        
        dados_para_verificar = {"produtor": produtor, "payload": envelope["payload"]}
        dados_bytes = json.dumps(dados_para_verificar, sort_keys=True).encode('utf-8')
        
        public_key.verify(signature_bytes, dados_bytes, padding.PKCS1v15(), hashes.SHA256())
        return True
    except Exception as e:
        print(f"    Falha na verificação da assinatura: {e}")
        return False