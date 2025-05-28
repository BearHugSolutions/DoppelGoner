// context/auth-context.tsx
"use client";

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';

interface User {
  id: string; // Added user ID
  username: string;
  email?: string | null; // Email is now part of the user object
  userSchema: string; // User's specific schema name
}

interface AuthContextType {
  currentUser: User | null;
  isLoading: boolean;
  login: (usernameInput: string, passwordInput: string) => Promise<void>;
  // Updated register function signature
  register: (usernameInput: string, passwordInput: string, emailInput: string) => Promise<void>; 
  logout: () => void;
  error: string | null;
  clearError: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [currentUser, setCurrentUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    try {
      const storedUser = localStorage.getItem('currentUser');
      if (storedUser) {
        const parsedUser = JSON.parse(storedUser);
        // Basic validation of stored user structure
        if (parsedUser && parsedUser.username && parsedUser.id && parsedUser.userSchema) {
            setCurrentUser(parsedUser);
        } else {
            localStorage.removeItem('currentUser'); // Clear invalid stored user
        }
      }
    } catch (e) {
      console.error("Failed to parse user from localStorage", e);
      localStorage.removeItem('currentUser');
    }
    setIsLoading(false);
  }, []);

  const clearError = () => setError(null);

  const login = async (usernameInput: string, passwordInput: string) => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: usernameInput, password: passwordInput }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Login failed');
      }
      // Ensure the user object from API matches the User interface
      if (data.user && data.user.id && data.user.username && data.user.userSchema) {
        setCurrentUser(data.user);
        localStorage.setItem('currentUser', JSON.stringify(data.user));
      } else {
        throw new Error('Login response did not include valid user data.');
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : 'An unknown error occurred during login.';
      setError(message);
      console.error("Login error:", err); // Keep console error for debugging
    } finally {
      setIsLoading(false);
    }
  };

  // Updated register function to accept email
  const register = async (usernameInput: string, passwordInput: string, emailInput: string) => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await fetch('/api/auth/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        // Pass email to the API
        body: JSON.stringify({ username: usernameInput, password: passwordInput, email: emailInput }), 
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Registration failed');
      }
      // Display success message, user will need to login separately
      alert(data.message || "Registration successful! Please log in."); 
      // Do not auto-login here to keep flows distinct
    } catch (err) {
      const message = err instanceof Error ? err.message : 'An unknown error occurred during registration.';
      setError(message);
      console.error("Registration error:", err); // Keep console error for debugging
    } finally {
      setIsLoading(false);
    }
  };

  const logout = () => {
    setCurrentUser(null);
    localStorage.removeItem('currentUser');
    setIsLoading(false);
    // Consider redirecting to login page: window.location.href = '/login'; (if you have a dedicated login page)
  };

  return (
    <AuthContext.Provider value={{ currentUser, isLoading, login, register, logout, error, clearError }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
