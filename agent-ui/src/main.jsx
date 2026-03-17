/**
 * Entry point for the EC2 Analysis Agent React application.
 * Initializes the root React tree and attaches it to the DOM.
 */
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './AppLight.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
