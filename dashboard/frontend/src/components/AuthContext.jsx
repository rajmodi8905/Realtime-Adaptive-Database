import { createContext, useContext, useState, useCallback, useEffect } from 'react'
import { api } from '../api'

const AuthContext = createContext(null)

const STORAGE_KEY = 'raddb_auth'

function loadFromStorage() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (raw) return JSON.parse(raw)
  } catch { /* ignore */ }
  return null
}

function saveToStorage(data) {
  if (data) localStorage.setItem(STORAGE_KEY, JSON.stringify(data))
  else localStorage.removeItem(STORAGE_KEY)
}

export function AuthProvider({ children }) {
  const [auth, setAuth] = useState(loadFromStorage)

  useEffect(() => { saveToStorage(auth) }, [auth])

  const login = useCallback(async (username) => {
    const res = await api.authLogin(username)
    if (res.success) {
      const session = { token: res.data.token, username: res.data.username }
      setAuth(session)
      return { success: true }
    }
    return { success: false, error: res.error }
  }, [])

  const logout = useCallback(async () => {
    if (auth?.token) {
      await api.authLogout(auth.token).catch(() => {})
    }
    setAuth(null)
  }, [auth])

  return (
    <AuthContext.Provider value={{ auth, login, logout, isAuthenticated: !!auth }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
