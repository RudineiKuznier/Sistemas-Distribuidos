import { useEffect, useRef, useState, useRef } from 'react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
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

interface EventoPedido {
  status: string
  routingKey?: string
  estoqueConfirmado?: boolean
  estoqueDevolvido?: boolean
  pagamentoPendente?: boolean
  cancelamentoSolicitado?: boolean
  notaFiscalEmitida?: boolean
}

interface Props {
  orderId: string
  onInventoryChanged: () => void
}

export function StatusPedido({ orderId, onInventoryChanged }: Props) {
  const [status, setStatus] = useState<string | null>(null)
  const [estoqueDevolvido, setEstoqueDevolvido] = useState(false)
  const [pagamentoPendente, setPagamentoPendente] = useState(false)
  const [pagamentoRecusado, setPagamentoRecusado] = useState(false)
  const [notaFiscalEmitida, setNotaFiscalEmitida] = useState(false)
  const [cancelamentoSolicitado, setCancelamentoSolicitado] = useState(false)
  const [cancelando, setCancelando] = useState(false)
  const [erroCancelamento, setErroCancelamento] = useState<string | null>(null)
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
      if (evento.pagamentoPendente) setPagamentoPendente(true)
      if (evento.notaFiscalEmitida) setNotaFiscalEmitida(true)
      if (evento.cancelamentoSolicitado) setCancelamentoSolicitado(true)
      if (precisaAtualizarCatalogo) onInventoryChanged()
    }

    return () => source.close() // fecha ao desmontar
  }, [orderId, onInventoryChanged])

  async function cancelarPedido() {
    if (!window.confirm('Deseja cancelar este pedido?')) return

    setCancelando(true)
    setErroCancelamento(null)
    try {
      const response = await fetch(`/api/orders/${orderId}/cancel`, { method: 'POST' })
      const resposta = await response.json()
      if (!response.ok) {
        throw new Error(resposta.error ?? 'Não foi possível cancelar o pedido.')
      }
      setCancelamentoSolicitado(true)
    } catch (erro) {
      setErroCancelamento(
        erro instanceof Error ? erro.message : 'Erro ao cancelar o pedido.'
      )
    } finally {
      setCancelando(false)
    }
  }

  const pedidoFinalizado = [
    'Pagamento Aprovado',
    'Pagamento Recusado',
    'Pedido Enviado',
    'Pedido Cancelado',
  ].includes(status ?? '')

  return (
    <Card>
      <CardHeader>
        <CardTitle>Pedido {orderId}</CardTitle>
        <CardDescription>Acompanhamento em tempo real</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap items-center gap-3">
          <Badge variant={pagamentoRecusado ? 'destructive' : 'secondary'}>
            {status ? ROTULOS[status] ?? status : 'Aguardando pagamento…'}
          </Badge>
          {notaFiscalEmitida && (
            <Badge variant="secondary">Nota Fiscal emitida</Badge>
          )}
          {pagamentoPendente && !estoqueDevolvido && !pedidoFinalizado && (
            <Button
              variant="destructive"
              disabled={cancelando}
              onClick={() => void cancelarPedido()}
            >
              {cancelando
                ? 'Cancelando…'
                : cancelamentoSolicitado
                  ? 'Reenviar cancelamento'
                  : 'Cancelar pedido'}
            </Button>
          )}
        </div>
        {erroCancelamento && (
          <p className="mt-2 text-sm text-destructive" role="alert">
            {erroCancelamento}
          </p>
        )}
        {estoqueDevolvido && (
          <p className="mt-2 text-sm">
            {cancelamentoSolicitado
              ? 'Pedido cancelado; itens devolvidos ao estoque.'
              : 'Itens devolvidos ao estoque.'}
          </p>
        )}
      </CardContent>
    </Card>
  )
}