import { useEffect, useState, useRef } from 'react'
import { Badge } from '@/components/ui/badge'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'

const ROTULOS: Record<string, string> = {
  'Estoque Confirmado': 'Estoque confirmado',
  'Estoque Indisponível': 'Estoque indisponível',
  'Pagamento Aprovado': 'Pagamento aprovado',
  'Pagamento Recusado': 'Pagamento recusado',
  'Pedido Enviado': 'Pedido enviado',
  CRIADO: 'Pedido criado',
  AGUARDANDO: 'Aguardando…',
}

const ERROS = ['Pagamento Recusado', 'Estoque Indisponível']

interface Props {
  orderId: string
  onStatusChange?: () => void
}

export function StatusPedido({ orderId, onStatusChange }: Props) {
  const [status, setStatus] = useState<string | null>(null)

  // Mantém o callback mais recente sem reabrir a conexão SSE
  const callbackRef = useRef(onStatusChange)
  useEffect(() => {
    callbackRef.current = onStatusChange
  }, [onStatusChange])

  useEffect(() => {
    const source = new EventSource(`/api/orders/${orderId}/events`)

    source.onmessage = (e) => {
      const evento = JSON.parse(e.data)
      setStatus(evento.status)
      callbackRef.current?.() // recarrega o catálogo a cada evento
    }

    return () => source.close()
  }, [orderId])

  return (
    <Card>
      <CardHeader>
        <CardTitle>Pedido {orderId}</CardTitle>
        <CardDescription>Acompanhamento em tempo real</CardDescription>
      </CardHeader>
      <CardContent>
        <Badge
  variant={status && ERROS.includes(status) ? 'destructive' : 'secondary'}
  className={
    status && !ERROS.includes(status)
      ? 'bg-green-50 text-green-700 dark:bg-green-950 dark:text-green-300'
      : ''
  }
>
  {status ? ROTULOS[status] ?? status : 'Aguardando pagamento…'}
</Badge>
        
      </CardContent>
    </Card>
  )
}