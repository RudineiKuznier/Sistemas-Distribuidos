import { useEffect, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"

interface EventoPedido {
  status?: string
  checkoutUrl?: string | null
}

export function MockPagamento() {
  const parametros = new URLSearchParams(location.search)
  const orderId = parametros.get("orderId")
  const chargeId = parametros.get("chargeId")
  const [enviando, setEnviando] = useState<"APROVADO" | "RECUSADO" | null>(null)
  const [feedback, setFeedback] = useState<{ mensagem: string; erro: boolean } | null>(null)
  const [finalizado, setFinalizado] = useState(false)

  useEffect(() => {
    if (!orderId || chargeId) return

    const source = new EventSource(`/api/orders/${orderId}/events`)
    source.onmessage = (event) => {
      const dados: EventoPedido = JSON.parse(event.data)
      if (dados.checkoutUrl) {
        source.close()
        location.replace(dados.checkoutUrl)
      } else if (dados.status === "Estoque Indisponível") {
        source.close()
        setFeedback({ mensagem: "Não foi possível reservar os itens.", erro: true })
      } else if (dados.status === "Pedido Cancelado") {
        source.close()
        setFeedback({ mensagem: "Este pedido foi cancelado.", erro: true })
      }
    }
    source.onerror = () => {
      setFeedback({
        mensagem: "Não foi possível acompanhar o pedido.",
        erro: true,
      })
    }

    return () => source.close()
  }, [orderId, chargeId])

  async function enviar(resultado: "APROVADO" | "RECUSADO") {
    if (!chargeId) {
      setFeedback({ mensagem: "Cobrança não identificada.", erro: true })
      return
    }

    setEnviando(resultado)
    setFeedback(null)

    try {
      const res = await fetch(`/mock-api/checkout/${chargeId}/result`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ result: resultado }),
      })
      const resposta = await res.json()
      if (!res.ok) {
        throw new Error(resposta.error ?? "Não foi possível enviar o resultado do pagamento.")
      }

      const texto = resultado === "APROVADO" ? "aprovado" : "recusado"
      setFinalizado(true)
      setFeedback({
        mensagem: resposta.duplicate
          ? `O pagamento já havia sido ${texto}.`
          : `Resultado de pagamento ${texto} enviado.`,
        erro: false,
      })
    } catch (e) {
      setFeedback({
        mensagem: e instanceof Error ? e.message : "Erro ao enviar o resultado do pagamento.",
        erro: true,
      })
    } finally {
      setEnviando(null)
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center p-4">
      <Card className="w-full max-w-sm">
        <CardHeader>
          <CardTitle>Pagamento simulado</CardTitle>
          <CardDescription>
            {chargeId
              ? "Escolha o resultado do pagamento."
              : "Aguardando a confirmação do estoque e a criação da cobrança."}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <Badge variant="secondary">Pedido {orderId ?? "inválido"}</Badge>
          {feedback && (
            <p
              className={`mt-3 text-sm ${feedback.erro ? "text-destructive" : "text-foreground"}`}
              role={feedback.erro ? "alert" : "status"}
            >
              {feedback.mensagem}
            </p>
          )}
        </CardContent>
        {chargeId && (
          <CardFooter className="flex gap-2">
            <Button
              className="flex-1"
              disabled={enviando !== null || finalizado}
              onClick={() => void enviar("APROVADO")}
            >
              {enviando === "APROVADO" ? "Enviando…" : "Aprovar"}
            </Button>
            <Button
              className="flex-1"
              variant="destructive"
              disabled={enviando !== null || finalizado}
              onClick={() => void enviar("RECUSADO")}
            >
              {enviando === "RECUSADO" ? "Enviando…" : "Recusar"}
            </Button>
          </CardFooter>
        )}
      </Card>
    </div>
  )
}
