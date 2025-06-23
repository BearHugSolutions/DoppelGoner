// context/auth-context.tsx
"use client";

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';

interface User {
  id: string;
  username: string;
  email?: string | null;
  teamId: string;          // New: team ID
  teamName: string;        // New: team name
  teamSchema: string;      // New: team schema name (e.g., "alpha")
  userPrefix: string;      // New: user prefix within team (e.g., "john")
}

// Team context type that matches the gateway client
export interface TeamContext {
  teamSchema: string;
  userPrefix: string;
}

interface AuthContextType {
  currentUser: User | null;
  isLoading: boolean;
  login: (usernameInput: string, passwordInput: string) => Promise<boolean>; // Changed return type to Promise<boolean>
  register: (usernameInput: string, passwordInput: string, emailInput: string, teamName: string, datasets?: string[]) => Promise<boolean>; // Changed return type to Promise<boolean>
  logout: () => void;
  error: string | null;
  clearError: () => void;
  // Helper function to get team context for API calls
  getTeamContext: () => TeamContext | null;
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
        // Validate the stored user structure for the new team-based system
        if (parsedUser &&
            parsedUser.username &&
            parsedUser.id &&
            parsedUser.teamSchema &&
            parsedUser.userPrefix &&
            parsedUser.teamName) {
          setCurrentUser(parsedUser);
        } else {
          console.warn("Stored user data is incompatible with team-based system, clearing...");
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

  // Helper function to extract team context for API calls
  const getTeamContext = (): TeamContext | null => {
    if (!currentUser) return null;
    return {
      teamSchema: currentUser.teamSchema,
      userPrefix: currentUser.userPrefix
    };
  };

  const login = async (usernameInput: string, passwordInput: string): Promise<boolean> => {
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

      // Validate the user object from API matches the new User interface
      if (data.user &&
          data.user.id &&
          data.user.username &&
          data.user.teamSchema &&
          data.user.userPrefix &&
          data.user.teamName) {
        setCurrentUser(data.user);
        localStorage.setItem('currentUser', JSON.stringify(data.user));
        return true; // Login successful
      } else {
        console.error("Login response user data:", data.user);
        throw new Error('Login response did not include valid team-based user data.');
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : 'An unknown error occurred during login.';
      setError(message);
      console.error("Login error:", err);
      return false; // Login failed
    } finally {
      setIsLoading(false);
    }
  };

  const register = async (usernameInput: string, passwordInput: string, emailInput: string, teamName: string, datasets?: string[]): Promise<boolean> => {
    setIsLoading(true);
    setError(null);
    try {
      console.log("Request body:", {
        username: usernameInput,
        password: passwordInput,
        email: emailInput,
        teamName: teamName,
        datasets: datasets
      });
      const response = await fetch('/api/auth/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: usernameInput,
          password: passwordInput,
          email: emailInput,
          teamName: teamName,
          datasets: datasets
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || 'Registration failed');
      }

      // Instead of alert(), you might want to integrate with a global toast system here
      // For now, we'll just return true and let auth-forms handle the success toast.
      // alert(data.message || "Registration successful! Please log in.");
      return true; // Registration successful
    } catch (err) {
      const message = err instanceof Error ? err.message : 'An unknown error occurred during registration.';
      setError(message);
      console.error("Registration error:", err);
      return false; // Registration failed
    } finally {
      setIsLoading(false);
    }
  };

  const logout = () => {
    setCurrentUser(null);
    localStorage.removeItem('currentUser');
    setIsLoading(false);
  };

  return (
    <AuthContext.Provider value={{
      currentUser,
      isLoading,
      login,
      register,
      logout,
      error,
      clearError,
      getTeamContext
    }}>
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