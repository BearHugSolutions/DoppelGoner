"use client";
import { useAuth } from "@/context/auth-context";
import { AuthForms } from "./auth-forms";
import { Button } from "../ui/button";

// Create a client component to conditionally render children or AuthForms
export default function AppContent({ children }: { children: React.ReactNode }) {
    const { currentUser, isLoading, logout } = useAuth();
  
    if (isLoading) {
      return (
        <div className="flex justify-center items-center h-screen">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-gray-900"></div>
        </div>
      );
    }
  
    if (!currentUser) {
      return <AuthForms />;
    }
  
    return (
      <div className="h-screen flex flex-col">
        <header className="p-4 bg-gray-800 text-white flex justify-between items-center flex-shrink-0">
          <span>Welcome, {currentUser.username}!</span>
          <Button onClick={logout} variant="destructive" size="sm">Logout</Button>
        </header>
        <div className="flex-1 min-h-0">
          {children}
        </div>
      </div>
    );
}