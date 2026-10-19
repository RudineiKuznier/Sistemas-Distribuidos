import { useEffect, useState } from 'react'
import { Badge } from '@/components/ui/badge'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'

const ROTULOS: Record<string, string> = {
  ESTOQUE_CONFIRMADO: 'Estoque confirmado',
  PAGAMENTO_APROVADO: 'Pagamento aprovado',
  PAGAMENTO_RECUSADO: 'Pagamento recusado',
  PEDIDO_ENVIADO: 'Pedido enviado',
}

export function StatusPedido({ orderId }: { orderId: string }) {
  const [status, setStatus] = useState<string | null>(null)

  useEffect(() => {
    const source = new EventSource(`/api/orders/${orderId}/events`)

    source.onmessage = (e) => {
      const evento = JSON.parse(e.data)
      setStatus(evento.status)
    }

    return () => source.close() // fecha ao desmontar
  }, [orderId])

  return (
    <Card>
      <CardHeader>
        <CardTitle>Pedido {orderId}</CardTitle>
        <CardDescription>Acompanhamento em tempo real</CardDescription>
      </CardHeader>
      <CardContent>
        <Badge variant={status === 'PAGAMENTO_RECUSADO' ? 'destructive' : 'secondary'}>
          {status ? ROTULOS[status] ?? status : 'Aguardando pagamento…'}
        </Badge>
      </CardContent>
    </Card>
  )
}