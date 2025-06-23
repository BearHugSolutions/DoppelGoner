// context/auth-context.tsx
"use client";

import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import { useToast } from '@/hooks/use-toast'; // Assuming you have this toast hook
import { useRouter } from 'next/navigation'; // Assuming you are using Next.js 13+ app router

// Define user and team types for clarity in the context
interface TeamContext {
  teamSchema: string;
  userPrefix: string;
}

interface User {
  id: string;
  username: string;
  email?: string;
  teamId: string;
  teamName: string;
  teamSchema: string;
  userPrefix: string;
}

interface AuthContextType {
  user: User | null;
  isLoading: boolean;
  error: string | null;
  login: (username: string, password: string) => Promise<boolean>;
  register: (username: string, password: string, email: string, teamName: string, datasets?: string[]) => Promise<boolean>;
  logout: () => void;
  clearError: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const { toast } = useToast();
  const router = useRouter();

  // Effect to load user from local storage on initial app load
  useEffect(() => {
    try {
      const storedUser = localStorage.getItem('user');
      if (storedUser) {
        setUser(JSON.parse(storedUser));
      }
    } catch (e) {
      console.error("Failed to parse user from local storage:", e);
      localStorage.removeItem('user'); // Clear corrupted storage
    }
  }, []);

  /**
   * Clears any authentication-related errors.
   */
  const clearError = useCallback(() => {
    setError(null);
  }, []);

  /**
   * Handles user login by calling the Next.js local API route.
   * @param username The user's username.
   * @param password The user's password.
   * @returns A boolean indicating login success or failure.
   */
  const login = useCallback(async (username: string, password: string): Promise<boolean> => {
    setIsLoading(true);
    clearError();
    try {
      // Call your Next.js API route for login
      const response = await fetch('/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username, password }),
      });

      const data = await response.json();
      // Added console log to inspect the data received from the login API
      console.log("Login API response data:", data); 

      if (response.ok) {
        // Ensure data.user exists before setting state and local storage
        if (data.user) { 
          setUser(data.user);
          try {
            localStorage.setItem('user', JSON.stringify(data.user)); // Persist user data
            // Confirmed user data saved to local storage
            console.log("User data saved to local storage:", data.user); 
          } catch (localStorageError) {
            console.error("Error saving user to local storage:", localStorageError);
            // Optionally, handle this error more gracefully, e.g., by informing the user
          }
          toast({
            title: "Login Successful",
            description: "Welcome back!",
          });
          router.push('/dashboard'); // Redirect to dashboard or a relevant page after successful login
          return true;
        } else {
          // Case where login is "ok" but user data is missing (unexpected but handled)
          setError('Login successful, but no user data received.');
          toast({
            title: "Login Error",
            description: "No user data received after successful login.",
            variant: "destructive",
          });
          return false;
        }
      } else {
        // Set error message from the API response
        setError(data.error || 'Login failed.');
        toast({
          title: "Login Error",
          description: data.error || 'Login failed. Please check your credentials.',
          variant: "destructive",
        });
        return false;
      }
    } catch (err: any) {
      console.error('Login network or unexpected error:', err);
      // Handle network errors or other unexpected issues
      setError(err.message || 'An unexpected error occurred during login.');
      toast({
        title: "Login Error",
        description: err.message || 'Network error occurred. Please try again.',
        variant: "destructive",
      });
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [clearError, toast, router]);

  /**
   * Handles user registration by calling the Next.js local API route.
   * THIS IS THE CRITICAL FIX: Ensures the call goes to your Next.js proxy.
   * @param username The desired username.
   * @param password The desired password.
   * @param email The user's email address.
   * @param teamName The name of the team to join or create.
   * @param datasets Optional array of dataset names for new teams.
   * @returns A boolean indicating registration success or failure.
   */
  const register = useCallback(async (username: string, password: string, email: string, teamName: string, datasets?: string[]): Promise<boolean> => {
    setIsLoading(true);
    clearError();
    try {
      // >>> CRITICAL CHANGE <<<
      // Call the local Next.js API route, which then proxies to your Gateway
      const response = await fetch('/api/auth/register', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username, password, email, teamName, datasets }),
      });

      const data = await response.json();

      if (response.ok) {
        toast({
          title: "Registration Successful",
          description: "Your account has been created. You can now log in.",
        });
        // You might want to automatically redirect to the login tab in AuthForms here
        return true;
      } else {
        // Set error message from the API response
        setError(data.error || 'Registration failed.');
        toast({
          title: "Registration Error",
          description: data.error || 'Registration failed. Please check the details and try again.',
          variant: "destructive",
        });
        return false;
      }
    } catch (err: any) {
      console.error('Registration network or unexpected error:', err);
      // Handle network errors or other unexpected issues
      setError(err.message || 'An unexpected error occurred during registration.');
      toast({
        title: "Registration Error",
        description: err.message || 'Network error occurred. Please try again.',
        variant: "destructive",
      });
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [clearError, toast]);

  /**
   * Logs out the current user, clears local storage, and redirects to login.
   */
  const logout = useCallback(() => {
    setUser(null);
    localStorage.removeItem('user');
    toast({
      title: "Logged out",
      description: "You have been successfully logged out.",
    });
    router.push('/login'); // Redirect to login page after logout
  }, [toast, router]);

  // Memoize the context value to prevent unnecessary re-renders
  const value = React.useMemo(() => ({
    user,
    isLoading,
    error,
    login,
    register,
    logout,
    clearError,
  }), [user, isLoading, error, login, register, logout, clearError]);

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
