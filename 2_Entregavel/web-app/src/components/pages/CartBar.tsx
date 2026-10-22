import { Button } from '@/components/ui/button'

interface Props {
  itens: number
  onFinalizar: () => void
  onEsvaziarCarrinho: () => void
}

export function CartBar({ itens, onFinalizar, onEsvaziarCarrinho }: Props) {
  if (itens === 0) return null

  return (
    <div className="fixed inset-x-0 bottom-0 z-50 flex items-center justify-between border-t bg-background p-4">
      <span>{itens} itens</span>

      <div className="flex gap-2">
        <Button variant="outline" onClick={onEsvaziarCarrinho}>
          Esvaziar carrinho
        </Button>
        <Button onClick={onFinalizar}>Finalizar e pagar</Button>
      </div>
    </div>
  )
}