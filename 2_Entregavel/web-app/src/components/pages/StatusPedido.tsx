import { useEffect, useState, useRef } from 'react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { DialogEmail } from '@/components/pages/DialogEmail'

const ROTULOS: Record<string, string> = {
  'Estoque Confirmado': 'Estoque confirmado',
  'Estoque Indisponível': 'Estoque indisponível',
  'Pagamento Aprovado': 'Pagamento aprovado',
  'Pagamento Recusado': 'Pagamento recusado',
  'Pedido Enviado': 'Pedido enviado',
}

const ERROS = ['Pagamento Recusado', 'Estoque Indisponível']
const INICIAIS = ['CRIADO', 'AGUARDANDO']

interface Props {
  orderId: string | null
  categorias: string[]
  onStatusChange?: () => void
}

export function StatusPedido({ orderId, categorias, onStatusChange }: Props) {
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

  const erro = status ? ERROS.includes(status) : false

  return (
  <Card>
    <CardHeader>
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
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