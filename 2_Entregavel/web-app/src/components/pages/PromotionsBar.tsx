import { Button } from '@/components/ui/button'

interface Props {
  onAbrirPromocoes: () => void
}

export function PromotionsBar({ onAbrirPromocoes }: Props) {
  return (
    <div className="fixed inset-x-0 top-0 z-50 flex items-center justify-end border-b bg-background p-4">
      <Button variant="outline" onClick={onAbrirPromocoes}>
        Promoções
      </Button>
    </div>
  )
}
