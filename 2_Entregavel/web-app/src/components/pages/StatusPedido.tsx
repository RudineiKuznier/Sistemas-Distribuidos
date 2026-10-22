import { useEffect, useRef, useState } from 'react'
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

interface EventoPedido {
  status: string
  routingKey?: string
  estoqueConfirmado?: boolean
  estoqueDevolvido?: boolean
}

interface Props {
  orderId: string
  onInventoryChanged: () => void
}

export function StatusPedido({ orderId, onInventoryChanged }: Props) {
  const [status, setStatus] = useState<string | null>(null)
  const [estoqueDevolvido, setEstoqueDevolvido] = useState(false)
  const [pagamentoRecusado, setPagamentoRecusado] = useState(false)
  const estoqueConfirmadoRef = useRef(false)
  const estoqueDevolvidoRef = useRef(false)

  useEffect(() => {
    const source = new EventSource(`/api/orders/${orderId}/events`)

    source.onmessage = (e) => {
      const evento: EventoPedido = JSON.parse(e.data)
      const confirmouEstoque = Boolean(evento.estoqueConfirmado)
      const devolveuEstoque = Boolean(evento.estoqueDevolvido)
      const precisaAtualizarCatalogo =
        (confirmouEstoque && !estoqueConfirmadoRef.current) ||
        (devolveuEstoque && !estoqueDevolvidoRef.current)

      estoqueConfirmadoRef.current ||= confirmouEstoque
      estoqueDevolvidoRef.current ||= devolveuEstoque

      if (evento.routingKey !== 'estoque.devolvido') {
        setStatus(evento.status)
      }
      if (evento.routingKey === 'pagamento.recusado' || evento.status === 'Pagamento Recusado') {
        setPagamentoRecusado(true)
      }
      if (devolveuEstoque) setEstoqueDevolvido(true)
      if (precisaAtualizarCatalogo) onInventoryChanged()
    }

    return () => source.close() // fecha ao desmontar
  }, [orderId, onInventoryChanged])

  return (
    <Card>
      <CardHeader>
        <CardTitle>Pedido {orderId}</CardTitle>
        <CardDescription>Acompanhamento em tempo real</CardDescription>
      </CardHeader>
      <CardContent>
        <Badge variant={pagamentoRecusado ? 'destructive' : 'secondary'}>
          {status ? ROTULOS[status] ?? status : 'Aguardando pagamento…'}
        </Badge>
        {estoqueDevolvido && (
          <p className="mt-2 text-sm">Itens devolvidos ao estoque.</p>
        )}
      </CardContent>
    </Card>
  )
}