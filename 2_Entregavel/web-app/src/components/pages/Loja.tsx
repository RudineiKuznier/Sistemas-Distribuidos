import { useState, useEffect, useCallback } from 'react'
import { CardImage } from '@/components/pages/CardImage'
import { CartBar } from '@/components/pages/CartBar'
import { DialogEmail } from '@/components/pages/DialogEmail'
import { StatusPedido } from '@/components/pages/StatusPedido'

interface Produto {
  id: string
  name: string
  price: number
  category: string
  stock: number
}

export function Loja() {
  const [produtos, setProdutos] = useState<Produto[]>([])
  const [carrinho, setCarrinho] = useState<Record<string, number>>({})
  const [orderId, setOrderId] = useState<string | null>(null)
  const [dialogEmail, setDialogEmail] = useState(false)

  const carregarProdutos = useCallback(() => {
    return fetch('/api/products')
      .then((res) => {
        if (!res.ok) throw new Error('Não foi possível carregar os produtos')
        return res.json()
      })
      .then(setProdutos)
      .catch((e) => console.error(e.message))
  }, [])

  const atualizarCatalogo = useCallback(() => {
    void carregarProdutos()
  }, [carregarProdutos])

  useEffect(() => {
    void carregarProdutos()
  }, [carregarProdutos])

  const categorias = [...new Set(produtos.map((p) => p.category))]
  const itens = Object.values(carrinho).reduce((soma, q) => soma + q, 0)

  function adicionar(p: Produto) {
    setCarrinho((c) => {
      const atual = c[p.id] ?? 0
      if (atual >= p.stock) return c 
      return { ...c, [p.id]: atual + 1 }
    })
  }

  async function finalizar() {
    const aba = window.open('', '_blank') 

    try {
      const res = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          items: Object.entries(carrinho).map(([productId, quantity]) => ({
            productId,
            quantity,
          })),
        }),
      })
      if (!res.ok) throw new Error('Erro ao criar o pedido')

      const { orderId, paymentUrl } = await res.json()
      if (aba) aba.location.href = paymentUrl
      setOrderId(orderId) // SSE
      setCarrinho({})
    } catch (e) {
      aba?.close()
      console.error(e instanceof Error ? e.message : 'Erro ao criar o pedido')
    }
  }
  return (
    <>
      <main className="mx-auto max-w-7xl p-4 pb-24">
        {orderId && (
          <StatusPedido
            key={orderId}
            orderId={orderId}
            onInventoryChanged={atualizarCatalogo}
          />
        )}
         <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {produtos.map((p) => (
            <CardImage
              key={p.id}
              name={p.name}
              descr={`${p.category} · R$ ${p.price.toFixed(2)}`}
              quantidade={p.stock}
              onAdd={() => adicionar(p)}
            />
          ))}
        </div>
      </main>

      <CartBar
        itens={itens}
        onFinalizar={finalizar}
        onAbrirEmail={() => setDialogEmail(true)}
      />

      <DialogEmail
        open={dialogEmail}
        onOpenChange={setDialogEmail}
        categorias={categorias}
      />
    </>
  )
}