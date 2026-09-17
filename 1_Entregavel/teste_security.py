import json
import os
import copy
from security import assinar_evento, verificar_evento

# Definições do serviço
PRODUTOR = "ms_pagamento"
DIRETORIO_ATUAL = os.path.dirname(os.path.abspath(__file__))

# Como os arquivos estão no mesmo diretório:
CAMINHO_PRIVADA = os.path.join(DIRETORIO_ATUAL, f"{PRODUTOR}.pem")
CAMINHO_PUBLICAS = os.path.join(DIRETORIO_ATUAL, "chaves_publicas") # Pasta que contém o ms_pagamento.pub


def teste(nome, condicao):
    status = "PASSOU" if condicao else "FALHOU"
    print(f"[{status}] {nome}")
    return condicao


def main():
    resultados = []

    # 1. Caminho feliz: assina e verifica sem alterar nada
    payload = {"id_pedido": 123, "status": "ESTOQUE_OK"}
    envelope = assinar_evento(payload, PRODUTOR, CAMINHO_PRIVADA)
    resultados.append(teste(
        "Evento assinado corretamente é validado",
        verificar_evento(envelope, CAMINHO_PUBLICAS) is True
    ))

    # 2. Payload adulterado depois de assinado -> deve falhar
    envelope_adulterado = copy.deepcopy(envelope)
    envelope_adulterado["payload"]["id_pedido"] = 999
    resultados.append(teste(
        "Payload adulterado é rejeitado",
        verificar_evento(envelope_adulterado, CAMINHO_PUBLICAS) is False
    ))

    # 3. Campo produtor trocado (ataque de impersonação) -> deve falhar
    envelope_produtor_trocado = copy.deepcopy(envelope)
    envelope_produtor_trocado["produtor"] = "ms_estoque"  # Troca para outro nome para testar a falha
    resultados.append(teste(
        "Troca do campo 'produtor' é rejeitada",
        verificar_evento(envelope_produtor_trocado, CAMINHO_PUBLICAS) is False
    ))

    # 4. Assinatura corrompida -> deve falhar
    envelope_sig_corrompida = copy.deepcopy(envelope)
    envelope_sig_corrompida["Signature"] = envelope_sig_corrompida["Signature"][:-4] + "AAAA"
    resultados.append(teste(
        "Assinatura corrompida é rejeitada",
        verificar_evento(envelope_sig_corrompida, CAMINHO_PUBLICAS) is False
    ))

    # 5. Chave pública do produtor não existe na pasta -> deve falhar
    pasta_inexistente = os.path.join(DIRETORIO_ATUAL, "pasta_que_nao_existe")
    resultados.append(teste(
        "Chave pública ausente é tratada sem crash",
        verificar_evento(envelope, pasta_inexistente) is False
    ))

    print()
    print(f"{sum(resultados)}/{len(resultados)} testes passaram")


if __name__ == "__main__":
    main()