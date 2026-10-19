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
}

const BADGES = [
  { id: 1, label: "Promoção 1" },
  { id: 2, label: "Promoção 2" },
  { id: 3, label: "Promoção 3" },
  { id: 4, label: "Promoção 4" },
]

export function DialogEmail({ open, onOpenChange }: Props) {
  function salvar(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault()
    const dados = new FormData(e.currentTarget)
    console.log(Object.fromEntries(dados))
    onOpenChange(false) 
  }
  const [selectedBadge, setSelectedBadge] = useState<number[]>([]);

  const toggleBadge = (index: number) => {
    setSelectedBadge((prev) =>
    prev.includes(index) ? prev.filter((i) => i !== index) : [...prev, index]
    );
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-sm">
        <form onSubmit={salvar} className="grid gap-4">
          <DialogHeader>
            <DialogTitle>Promoções</DialogTitle>
            <DialogDescription>
              
            </DialogDescription>
          </DialogHeader>

          <FieldGroup>
            <Field>
              <Label htmlFor="name-1">Name</Label>
              <Input id="name-1" name="name"/>
            </Field>
            <Field>
              <Label htmlFor="username-1">Categorias</Label>
                <div className="flex w-full flex-wrap justify-center gap-2">
                  {BADGES.map((badge) => (
                    <Badge
                      key={badge.id}
                      className="cursor-pointer"
                      variant={selectedBadge.includes(badge.id) ? "default" : "outline"}
                      onClick={() => toggleBadge(badge.id)}
                    >
                      {badge.label}
                    </Badge>
                  ))}
                </div>
            </Field>
          </FieldGroup>

          <DialogFooter>
            <DialogClose render={<Button type="button" variant="outline">Cancel</Button>} />
            <Button type="submit">Save changes</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}