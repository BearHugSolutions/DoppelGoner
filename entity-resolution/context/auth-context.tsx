// context/auth-context.tsx - LOGOUT AND SESSION HANDLING FIXES
"use client";

import React, {
  createContext,
  useContext,
  useState,
  useEffect,
  ReactNode,
  useCallback,
} from "react";
import { useToast } from "@/hooks/use-toast";

// Represents a single available opinion for a user.
export interface OpinionInfo {
  name: string;
  isDefault: boolean;
}

// Represents the authenticated user.
export interface User {
  id: string;
  username: string;
  email?: string;
  teamId: string;
  teamName: string;
  teamSchema: string;
  userPrefix: string;
  opinions: OpinionInfo[];
}

// Structure of the backend login API response.
interface LoginResponse {
  message: string;
  user: User;
  team: any;
}

// ✨ NEW: Auth status response structure
interface AuthStatusResponse {
  isLoggedIn: boolean;
  user: User | null;
  sessionCleared?: boolean; // Indicates if an invalid session was cleared
}

// Defines the shape of the authentication context.
interface AuthContextType {
  user: User | null;
  selectedOpinion: string | null;
  isLoading: boolean;
  error: string | null;
  login: (username: string, password: string) => Promise<boolean>;
  register: (
    username: string,
    password: string,
    email: string,
    teamName: string,
    datasets?: string[]
  ) => Promise<boolean>;
  logout: () => Promise<void>; // ✨ UPDATED: Now returns Promise<void>
  checkAuthStatus: () => Promise<void>;
  clearError: () => void;
  selectOpinion: (opinionName: string) => void;
}

// Create the context with an undefined default value.
const AuthContext = createContext<AuthContextType | undefined>(undefined);

// The provider component that wraps the application.
export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [selectedOpinion, setSelectedOpinion] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { toast } = useToast();

  /**
   * Clears any authentication-related errors from the state.
   */
  const clearError = () => setError(null);

  /**
   * ✨ UPDATED: Enhanced auth status check with better error handling
   * and detection of cleared sessions.
   */
  const checkAuthStatus = useCallback(async () => {
    setIsLoading(true);
    try {
      const res = await fetch("/api/auth/status", {
        // Add cache-busting to ensure we get fresh data
        cache: 'no-cache',
        headers: {
          'Cache-Control': 'no-cache, no-store, must-revalidate',
          'Pragma': 'no-cache'
        }
      });
      
      if (res.ok) {
        const data: AuthStatusResponse = await res.json();
        
        // ✨ Check if session was cleared (indicates stale session)
        if (data.sessionCleared) {
          console.log("Stale session was detected and cleared by server");
          toast({
            title: "Session Expired",
            description: "Your session has expired. Please log in again.",
            variant: "destructive",
          });
        }
        
        if (data.isLoggedIn && data.user) {
          setUser(data.user);
          // Set opinions and default selected opinion from session data
          const defaultOpinion =
            data.user.opinions?.find((o: OpinionInfo) => o.isDefault) ||
            data.user.opinions?.[0];
          setSelectedOpinion(defaultOpinion?.name || "default");
        } else {
          // Not logged in, clear user state
          setUser(null);
          setSelectedOpinion(null);
        }
      } else {
        console.warn("Auth status check failed:", res.status, res.statusText);
        // If status check fails, assume logged out
        setUser(null);
        setSelectedOpinion(null);
      }
    } catch (err) {
      console.error("Error checking auth status:", err);
      setError("Failed to check authentication status.");
      setUser(null);
      setSelectedOpinion(null);
    } finally {
      setIsLoading(false);
    }
  }, [toast]);

  // On component mount, check the authentication status.
  useEffect(() => {
    checkAuthStatus();
  }, [checkAuthStatus]);

  /**
   * Logs a user in by calling the backend API.
   * On success, it sets the user and opinion state.
   */
  const login = async (username: string, password: string): Promise<boolean> => {
    setIsLoading(true);
    clearError();
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });

      const data: LoginResponse | { error: string } = await res.json();

      if (res.ok && "user" in data) {
        setUser(data.user);
        // Set opinions and find the default to select it automatically
        const defaultOpinion =
          data.user.opinions?.find((o) => o.isDefault) ||
          data.user.opinions?.[0];
        setSelectedOpinion(defaultOpinion?.name || "default");

        toast({
          title: "Login Successful",
          description: `Welcome back, ${data.user.username}!`,
        });
        // A full page reload is often better after login to ensure all state is fresh
        window.location.href = "/";
        return true;
      } else {
        const errorMessage = (data as { error: string }).error || "Login failed.";
        setError(errorMessage);
        toast({
          title: "Login Failed",
          description: errorMessage,
          variant: "destructive",
        });
        return false;
      }
    } catch (err) {
      const errorMessage = (err as Error).message || "An unexpected error occurred.";
      setError(errorMessage);
      toast({
        title: "Login Error",
        description: errorMessage,
        variant: "destructive",
      });
      return false;
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * Registers a new user by calling the backend API.
   */
  const register = async (
    username: string,
    password: string,
    email: string,
    teamName: string,
    datasets?: string[]
  ): Promise<boolean> => {
    setIsLoading(true);
    clearError();
    try {
      const res = await fetch("/api/auth/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password, email, teamName, datasets }),
      });

      const data = await res.json();

      if (res.ok) {
        toast({
          title: "Registration Successful",
          description: "You can now log in with your new credentials.",
        });
        return true;
      } else {
        setError(data.error || "Registration failed.");
        toast({
          title: "Registration Failed",
          description: data.error,
          variant: "destructive",
        });
        return false;
      }
    } catch (err) {
      const errorMessage = (err as Error).message || "An unexpected error occurred.";
      setError(errorMessage);
      toast({
        title: "Registration Error",
        description: errorMessage,
        variant: "destructive",
      });
      return false;
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * ✨ UPDATED: Enhanced logout function with better error handling
   * and proper session clearing.
   */
  const logout = async (): Promise<void> => {
    setIsLoading(true);
    try {
      // Call the logout endpoint
      const response = await fetch("/api/auth/logout", { 
        method: "POST",
        cache: 'no-cache',
        headers: {
          'Cache-Control': 'no-cache'
        }
      });
      
      if (!response.ok) {
        console.warn("Logout API call failed, but continuing with client-side logout");
      }
      
      // Clear client-side state regardless of API response
      setUser(null);
      setSelectedOpinion(null);
      setError(null);
      
      toast({
        title: "Logged Out",
        description: "You have been successfully logged out.",
      });
      
    } catch (err) {
      console.error("Logout error:", err);
      // Even if logout API fails, clear client state
      setUser(null);
      setSelectedOpinion(null);
      setError(null);
      
      toast({
        title: "Logged Out",
        description: "You have been logged out.",
      });
    } finally {
      setIsLoading(false);
      
      // ✨ IMPROVED: Use window.location.replace instead of href to prevent back button issues
      // Small delay to ensure state updates and toast are processed
      setTimeout(() => {
        window.location.replace("/");
      }, 100);
    }
  };

  /**
   * Allows the user to switch their active opinion.
   * @param opinionName The name of the opinion to switch to.
   */
  const selectOpinion = (opinionName: string) => {
    if (user?.opinions.some((o) => o.name === opinionName)) {
      setSelectedOpinion(opinionName);
      toast({
        title: "Opinion Switched",
        description: `Now viewing the "${opinionName}" opinion.`,
      });
    } else {
      console.error(`Attempted to select non-existent opinion: ${opinionName}`);
      toast({
        title: "Error",
        description: "Could not switch to the selected opinion.",
        variant: "destructive",
      });
    }
  };

  // The value provided to consuming components.
  const value = {
    user,
    selectedOpinion,
    isLoading,
    error,
    login,
    register,
    logout,
    checkAuthStatus,
    clearError,
    selectOpinion,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

/**
 * Custom hook to easily access the authentication context.
 * Throws an error if used outside of an AuthProvider.
 */
export const useAuth = (): AuthContextType => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
};