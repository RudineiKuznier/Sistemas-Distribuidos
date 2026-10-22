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
  const [erro, setErro] = useState<string | null>(null)
  const [processando, setProcessando] = useState(false)

  const toggleBadge = (categoria: string) => {
    setSelectedBadge((prev) =>
      prev.includes(categoria)
        ? prev.filter((c) => c !== categoria)
        : [...prev, categoria]
    )
  }

  async function processar(cancelar = false) {
    setProcessando(true)
    setErro(null)

    try {
      const res = await fetch("/api/promotions/subscriptions", {
        method: cancelar ? "DELETE" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(
          cancelar
            ? { email }
            : { email, categories: selectedBadge }
        ),
      })
      const resposta = await res.json()
      if (!res.ok) throw new Error(resposta.error ?? "Não foi possível processar a solicitação.")

      setSelectedBadge([])
      setEmail("")
      onOpenChange(false)
    } catch (erro) {
      setErro(
        erro instanceof Error ? erro.message : "Não foi possível processar a solicitação."
      )
    } finally {
      setProcessando(false)
    }
  }

  function salvar(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault()
    void processar()
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-sm">
        <form onSubmit={salvar} className="grid gap-4">
          <DialogHeader>
            <DialogTitle>Promoções</DialogTitle>
            <DialogDescription>
              Selecione as categorias de promoções que deseja receber. Sem seleção,
              você receberá ofertas de todas as categorias.
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

          {erro && (
            <p className="text-sm text-destructive" role="alert">
              {erro}
            </p>
          )}

          <DialogFooter>
            <DialogClose render={<Button type="button" variant="outline">Fechar</Button>} />
            <Button
              type="button"
              variant="outline"
              disabled={processando}
              onClick={(e) => {
                const form = e.currentTarget.form
                if (form?.reportValidity()) {
                  void processar(true)
                }
              }}
            >
              Cancelar inscrição
            </Button>
            <Button type="submit" disabled={processando}>
              {processando ? "Enviando…" : "Inscrever"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}