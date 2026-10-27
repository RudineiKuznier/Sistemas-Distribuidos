import { useEffect, useState, useRef } from 'react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardHeader, CardTitle } from '@/components/ui/card'
import { DialogEmail } from '@/components/pages/DialogEmail'

const ROTULOS: Record<string, string> = {
  'Estoque Confirmado': 'Estoque confirmado',
  'Estoque Indisponível': 'Estoque indisponível',
  'Pagamento Aprovado': 'Pagamento aprovado',
  'Pagamento Recusado': 'Pagamento recusado',
  'Pedido Enviado': 'Pedido enviado',
  'Pedido Cancelado': 'Pedido cancelado',
}

const ERROS = ['Pagamento Recusado', 'Estoque Indisponível', 'Pedido Cancelado']
const INICIAIS = ['CRIADO', 'AGUARDANDO']

interface Props {
  orderId: string | null
  paymentUrl: string | null
  categorias: string[]
  onStatusChange?: () => void
}

export function StatusPedido({
  orderId,
  paymentUrl,
  categorias,
  onStatusChange,
}: Props) {
  const [status, setStatus] = useState<string | null>(null)
  const [dialogEmail, setDialogEmail] = useState(false)

  const callbackRef = useRef(onStatusChange)
  useEffect(() => {
    callbackRef.current = onStatusChange
  }, [onStatusChange])

  useEffect(() => {
    setStatus(null)
    if (!orderId) return

    const source = new EventSource(`/api/orders/${orderId}/events`)

    source.onmessage = (e) => {
      const evento = JSON.parse(e.data)
      if (!INICIAIS.includes(evento.status)) setStatus(evento.status)
      callbackRef.current?.()
    }

    return () => source.close()
  }, [orderId])

  async function cancelarPedido() {
    try {
      const res = await fetch(`/api/orders/${orderId}`, { method: 'DELETE' })
      if (!res.ok) throw new Error()
    } catch {
      console.error('Erro ao cancelar o pedido')
    }
  }

  const erro = status ? ERROS.includes(status) : false
  const aguardandoPagamento = status === 'Estoque Confirmado'

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-2">
          <div className="flex flex-wrap items-center gap-2">
            {orderId && status && (
              <>
                <CardTitle className="text-sm">Pedido {orderId}</CardTitle>
                <Badge
                  variant={erro ? 'destructive' : 'secondary'}
                  className={
                    erro
                      ? ''
                      : 'bg-green-50 text-green-700 dark:bg-green-950 dark:text-green-300'
                  }
                >
                  {ROTULOS[status] ?? status}
                </Badge>
                {aguardandoPagamento && (
                  <Button size="sm" variant="outline" onClick={cancelarPedido}>
                    Cancelar pedido
                  </Button>
                )}
              </>
            )}
          </div>
          <Button variant="default" onClick={() => setDialogEmail(true)}>
            Promoções
          </Button>
        </div>
      </CardHeader>

      <DialogEmail
        open={dialogEmail}
        onOpenChange={setDialogEmail}
        categorias={categorias}
      />
    </Card>
  )
}