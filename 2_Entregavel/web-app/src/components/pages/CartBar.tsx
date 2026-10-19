import { Button } from '@/components/ui/button'

interface Props {
  itens: number
  onFinalizar: () => void
  onAbrirEmail: () => void
}

export function CartBar({ itens, onFinalizar, onAbrirEmail }: Props) {
  if (itens === 0) return null

  return (
    <div className="fixed inset-x-0 bottom-0 z-50 flex items-center justify-between border-t bg-background p-4">
      <span>{itens} itens</span>

      <div className="flex gap-2">
        <Button onClick={onFinalizar}>Finalizar e pagar</Button>
        <Button variant="outline" onClick={onAbrirEmail}>
          Promoções
        </Button>
      </div>
    </div>
  )
}