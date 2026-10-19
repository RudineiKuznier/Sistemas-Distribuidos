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

  async function enviar(resultado: "APROVADO" | "RECUSADO") {
    await fetch(`/api/payments/${orderId}/result`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ result: resultado }),
    })
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
        </CardContent>
        <CardFooter className="flex gap-2">
          <Button className="flex-1" onClick={() => enviar("APROVADO")}>
            Aprovar
          </Button>
          <Button className="flex-1" variant="destructive" onClick={() => enviar("RECUSADO")}>
            Recusar
          </Button>
        </CardFooter>
      </Card>
    </div>
  )
}