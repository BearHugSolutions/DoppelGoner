// components/auth/app-content.tsx
"use client";
import { useAuth } from "@/context/auth-context";
import { AuthForms } from "./auth-forms";
import { Button } from "../ui/button";
import { Badge } from "../ui/badge";
import { Users, User, LogOut, BrainCircuit } from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

// Create a client component to conditionally render children or AuthForms
export default function AppContent({ children }: { children: React.ReactNode }) {
  const { user, isLoading, logout, selectedOpinion, selectOpinion } = useAuth();

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-screen bg-gray-50 dark:bg-gray-900">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 dark:border-blue-400"></div>
      </div>
    );
  }

  if (!user) {
    return <AuthForms />;
  }

  return (
    <div className="h-screen flex flex-col">
      <header className="p-4 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 flex justify-between items-center flex-shrink-0 shadow-sm">
        <div className="flex items-center space-x-6">
          {/* User Info */}
          <div className="flex items-center space-x-2">
            <User className="h-5 w-5 text-gray-600 dark:text-gray-300" />
            <span className="font-medium text-gray-900 dark:text-gray-100">{user.username}</span>
          </div>
          
          {/* Team Info */}
          <div className="flex items-center space-x-2">
            <Users className="h-4 w-4 text-gray-500 dark:text-gray-400" />
            <Badge variant="secondary" className="bg-gray-100 dark:bg-gray-700 text-gray-800 dark:text-gray-200">
              Team: {user.teamName}
            </Badge>
          </div>

          {/* ✨ Opinion Switcher Dropdown */}
          {user.opinions && user.opinions.length > 0 && (
            <div className="flex items-center space-x-2">
              <BrainCircuit className="h-4 w-4 text-gray-500 dark:text-gray-400" />
              <div className="flex flex-col">
                <Select
                  value={selectedOpinion || ""}
                  onValueChange={selectOpinion}
                >
                  <SelectTrigger className="w-[200px] h-9 bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600">
                    <SelectValue placeholder="Select Opinion" />
                  </SelectTrigger>
                  <SelectContent>
                    {user.opinions.map((opinion) => (
                      <SelectItem key={opinion.name} value={opinion.name}>
                        <div className="flex items-center">
                          <span className="font-medium">
                            {opinion.name}
                          </span>
                          {opinion.isDefault && (
                            <Badge variant="outline" className="ml-2 text-xs">
                              Default
                            </Badge>
                          )}
                        </div>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          )}
          
          {/* Optional: Show team schema for debugging/admin purposes */}
          {process.env.NODE_ENV === 'development' && (
            <Badge variant="outline" className="border-gray-400 dark:border-gray-500 text-gray-600 dark:text-gray-300 text-xs">
              Schema: {user.teamSchema}
            </Badge>
          )}
        </div>
        
        <Button 
          onClick={logout} 
          variant="ghost" 
          size="sm"
          className="text-gray-600 dark:text-gray-300 hover:bg-red-50 dark:hover:bg-red-900/20 hover:text-red-600 dark:hover:text-red-400"
        >
          <LogOut className="h-4 w-4 mr-2" />
          Logout
        </Button>
      </header>
      
      <div className="flex-1 min-h-0 bg-gray-50 dark:bg-gray-900">
        {children}
      </div>
    </div>
  );
}