import { useState } from "react"
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

export function MockPagamento() {
  const orderId = new URLSearchParams(location.search).get("orderId")
  const [enviando, setEnviando] = useState<"APROVADO" | "RECUSADO" | null>(null)
  const [feedback, setFeedback] = useState<{ mensagem: string; erro: boolean } | null>(null)
  const [finalizado, setFinalizado] = useState(false)

  async function enviar(resultado: "APROVADO" | "RECUSADO") {
    setEnviando(resultado)
    setFeedback(null)

    try {
      const res = await fetch(`/api/payments/${orderId}/result`, {
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
          <CardDescription>Escolha o resultado do pagamento.</CardDescription>
        </CardHeader>
        <CardContent>
          <Badge variant="secondary">Pedido {orderId}</Badge>
          {feedback && (
            <p className={`mt-3 text-sm ${feedback.erro ? "text-destructive" : "text-foreground"}`} role={feedback.erro ? "alert" : "status"}>
              {feedback.mensagem}
            </p>
          )}
        </CardContent>
        <CardFooter className="flex gap-2">
          <Button className="flex-1" disabled={enviando !== null || finalizado} onClick={() => void enviar("APROVADO")}>
            {enviando === "APROVADO" ? "Enviando…" : "Aprovar"}
          </Button>
          <Button className="flex-1" variant="destructive" disabled={enviando !== null || finalizado} onClick={() => void enviar("RECUSADO")}>
            {enviando === "RECUSADO" ? "Enviando…" : "Recusar"}
          </Button>
        </CardFooter>
      </Card>
    </div>
  )
}