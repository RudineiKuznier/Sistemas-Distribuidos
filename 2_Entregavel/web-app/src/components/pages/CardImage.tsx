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

interface CardImageProps {
  name: string
  quantidade: number
  descr: string 
  onAdd: ()=> void
}

export function CardImage({ name, quantidade, descr, onAdd }: CardImageProps) {
  return (
    <Card className="relative mx-auto w-full max-w pt-0">
      <div className="absolute inset-0 z-30 aspect-video bg-black/35" />
      <blockquote
        className="relative z-20 aspect-video w-full object-cover brightness-70 grayscale dark:brightness-40"
      />
      <CardHeader>
        <CardAction>
          <Badge variant="secondary">{quantidade}</Badge>
        </CardAction>
        <CardTitle>{name}</CardTitle>
        <CardDescription>
         {descr}
        </CardDescription>
      </CardHeader>
      <CardFooter>
        <Button className="w-full" onClick={onAdd}>
          Adicionar
        </Button>
      </CardFooter>
    </Card>
  )
}
