import { Loja } from './components/pages/Loja'
import { MockPagamento } from './components/pages/MockPagamento'

function App() {
  if (location.pathname === '/mock-pagamento') return <MockPagamento />
  return <Loja />
}

export default App