// components/auth/app-content.tsx
"use client";
import { useAuth } from "@/context/auth-context";
import { AuthForms } from "./auth-forms";
import { Button } from "../ui/button";
import { Badge } from "../ui/badge";
import { Users, User } from "lucide-react";

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
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-2">
            <User className="h-5 w-5" />
            <span className="font-medium">{currentUser.username}</span>
          </div>
          
          <div className="flex items-center space-x-2">
            <Users className="h-4 w-4 text-gray-300" />
            <Badge variant="secondary" className="bg-gray-600 text-white">
              Team: {currentUser.teamName}
            </Badge>
          </div>
          
          {/* Optional: Show team schema for debugging/admin purposes */}
          {process.env.NODE_ENV === 'development' && (
            <Badge variant="outline" className="border-gray-500 text-gray-300 text-xs">
              Schema: {currentUser.teamSchema}
            </Badge>
          )}
        </div>
        
        <Button onClick={logout} variant="destructive" size="sm">
          Logout
        </Button>
      </header>
      
      <div className="flex-1 min-h-0">
        {children}
      </div>
    </div>
  );
}