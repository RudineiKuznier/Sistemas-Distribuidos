import { Button } from '@/components/ui/button'

interface Props {
  itens: number
  onFinalizar: () => void
}

export function CartBar({ itens, onFinalizar }: Props) {
  if (itens === 0) return null

return(
  <div className="fixed inset-x-0 bottom-0 z-50 mx-auto w-full max-w-7xl border-t bg-background p-4 sm:rounded-t-lg sm:border">
    <div className="flex items-center justify-between">
      <span>{itens} itens</span>
      <Button onClick={onFinalizar}>Finalizar e pagar</Button>
    </div>
  </div>
)
}