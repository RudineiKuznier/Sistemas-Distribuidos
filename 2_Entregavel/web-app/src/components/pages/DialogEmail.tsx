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
  const [email, setEmail] = useState("")
  const [mensagem, setMensagem] = useState<string | null>(null)

  const toggleBadge = (categoria: string) => {
    setSelectedBadge((prev) =>
      prev.includes(categoria)
        ? prev.filter((c) => c !== categoria)
        : [...prev, categoria]
    )
  }

  function fechar(aberto: boolean) {
    if (!aberto) setMensagem(null)
    onOpenChange(aberto)
  }

  async function salvar(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault()
    try {
      const res = await fetch("/api/promotions/subscriptions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, categories: selectedBadge }),
      })
      if (!res.ok) throw new Error()

      setSelectedBadge([])
      setEmail("")
      fechar(false)
    } catch {
      setMensagem("Erro ao salvar a inscrição")
    }
  }

  async function cancelarInscricao() {
    if (!email) {
      setMensagem("Informe o e-mail para cancelar")
      return
    }
    try {
      const res = await fetch("/api/promotions/subscriptions", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email }),
      })
      if (!res.ok) throw new Error()

      setSelectedBadge([])
      setEmail("")
      fechar(false)
    } catch {
      setMensagem("Erro ao cancelar a inscrição")
    }
  }

  return (
    <Dialog open={open} onOpenChange={fechar}>
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
              <Input
                id="email-1"
                name="email"
                type="email"
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
              />
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

          {mensagem && <p className="text-sm text-destructive">{mensagem}</p>}

          <DialogFooter>
            <Button variant="outline" type="button" onClick={cancelarInscricao}>
              Cancelar inscrição
            </Button>
            <Button type="submit" disabled={selectedBadge.length === 0}>
              Inscrever
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}