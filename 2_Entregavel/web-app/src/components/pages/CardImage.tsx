import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardAction,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { cn } from "@/lib/utils"

interface CardImageProps {
  name: string
  quantidade: number
  descr: string
  onAdd: () => void
  disabled?: boolean
}

export function CardImage({
  name,
  quantidade,
  descr,
  onAdd,
  disabled = false,
}: CardImageProps) {
  return (
    <Card
      className={cn(
        "relative mx-auto w-full max-w pt-0",
        disabled && "opacity-50"
      )}
      aria-disabled={disabled}
    >
      <div className="absolute inset-0 z-30 aspect-video bg-black/35" />
      <blockquote
        className="relative z-20 aspect-video w-full object-cover brightness-70 grayscale dark:brightness-40"
      />
      <CardHeader>
        <CardAction>
          <Badge className="bg-blue-50 text-blue-700 dark:bg-blue-950 dark:text-blue-300">
            {quantidade}
          </Badge>
        </CardAction>
        <CardTitle>{name}</CardTitle>
        <CardDescription>{descr}</CardDescription>
      </CardHeader>
      <CardFooter>
        <Button className="w-full" onClick={onAdd} disabled={disabled}>
          {disabled ? "Esgotado" : "Adicionar"}
        </Button>
      </CardFooter>
    </Card>
  )
}