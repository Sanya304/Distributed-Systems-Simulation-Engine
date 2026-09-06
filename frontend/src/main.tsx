import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

const root = document.getElementById('root')!

try {
  createRoot(root).render(
    <StrictMode>
      <App />
    </StrictMode>,
  )
} catch (err) {

  root.innerHTML = `<pre style="color:red;padding:20px;background:#111;font-size:12px">${err}</pre>`
  console.error('React startup error:', err)
}
