import { useState } from 'react'
import { CardImage } from '@/components/pages/CardImage'
import { CartBar } from '@/components/pages/CartBar'
import { DialogEmail } from '@/components/pages/DialogEmail'
import { StatusPedido } from '@/components/pages/StatusPedido'

const PROD = [
  { id: 1, label: "Promoção 1" , descr:"teste1", qtde: 12},
  { id: 2, label: "Promoção 2" , descr:"teste2", qtde: 1},
  { id: 3, label: "Promoção 3" , descr:"teste3", qtde: 12},
  { id: 4, label: "Promoção 4" , descr:"teste4", qtde: 34},
]
export function Loja() {
  const [itens, setItens] = useState(0)
  const [dialogEmail, setDialogEmail] = useState(false)
  const [orderId, setOrderId] = useState<string | null>(null)

  function finalizar() {
  const id = '123' 
  window.open(`/mock-pagamento?orderId=${id}`, '_blank')
  setOrderId(id)
  setItens(0)
}

  return (
    <>
      <main className="mx-auto max-w-7xl p-4 pb-24">
        {orderId && <StatusPedido orderId={orderId} />}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {PROD.map((prod) => (
            <CardImage 
                key={prod.id}
                name={prod.label}
                descr={prod.descr}
                quantidade={prod.qtde}
                onAdd={() => setItens((n) => n + 1)} 
            />
          ))}
        </div>
      </main>

      <CartBar
        itens={itens}
        onFinalizar={finalizar}
        onAbrirEmail={() => setDialogEmail(true)}
      />

      <DialogEmail open={dialogEmail} onOpenChange={setDialogEmail} />
    </>
  )
}