import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Field, FieldGroup } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge} from "@/components/ui/badge"
import { useState } from "react"

interface Props {
  open: boolean
  onOpenChange: (open: boolean) => void
  categorias: string[]
}

export function DialogEmail({ open, onOpenChange, categorias }: Props) {
  const [selectedBadge, setSelectedBadge] = useState<string[]>([])

  const toggleBadge = (categoria: string) => {
    setSelectedBadge((prev) =>
      prev.includes(categoria)
        ? prev.filter((c) => c !== categoria)
        : [...prev, categoria]
    )
  }

  async function salvar(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault()
    const email = new FormData(e.currentTarget).get("email")

    try {
      const res = await fetch("/api/promotions/subscriptions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, categories: selectedBadge }),
      })
      if (!res.ok) throw new Error()

      setSelectedBadge([])
      onOpenChange(false)
    } catch {
      console.error("Erro ao salvar a inscrição")
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-sm">
        <form onSubmit={salvar} className="grid gap-4">
          <DialogHeader>
            <DialogTitle>Promoções</DialogTitle>
            <DialogDescription>
              Selecione as categorias de promoções que deseja receber
            </DialogDescription>
          </DialogHeader>

          <FieldGroup>
            <Field>
              <Label htmlFor="email-1">E-mail</Label>
              <Input id="email-1" name="email" type="email" />
            </Field>
            <Field>
              <Label>Categorias</Label>
                <div className="flex w-full flex-wrap justify-center gap-2">
                  {categorias.map((cat) => (
                    <Badge
                      key={cat}
                      className="cursor-pointer"
                      variant={selectedBadge.includes(cat) ? "default" : "outline"}
                      onClick={() => toggleBadge(cat)}
                    >
                      {cat}
                    </Badge>
                  ))}
                </div>
            </Field>
          </FieldGroup>

          <DialogFooter>
            <DialogClose render={<Button type="button" variant="outline">Cancelar</Button>} />
            <Button type="submit">Inscrever</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}