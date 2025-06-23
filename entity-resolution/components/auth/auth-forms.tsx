// components/auth-forms.tsx
"use client";

import React, { useState, FormEvent, useEffect } from 'react';
import { useAuth } from '@/context/auth-context';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { useToast } from '@/hooks/use-toast';

interface Team {
  id: string;
  name: string;
  displayName?: string;
}

interface Dataset {
  name: string;
  displayName: string;
  description?: string;
  entityCount?: number;
  isActive: boolean;
}

export function AuthForms() {
  const { login, register, isLoading, error, clearError } = useAuth();
  const { toast } = useToast();

  // Form state
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [email, setEmail] = useState('');
  const [isLoginTab, setIsLoginTab] = useState(true);

  // Team-related state
  const [teamMode, setTeamMode] = useState<'existing' | 'new'>('existing');
  const [selectedTeam, setSelectedTeam] = useState<string>('');
  const [newTeamName, setNewTeamName] = useState('');
  const [selectedDatasets, setSelectedDatasets] = useState<string[]>([]);

  // Data loading state
  const [teams, setTeams] = useState<Team[]>([]);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [loadingTeams, setLoadingTeams] = useState(false);
  const [loadingDatasets, setLoadingDatasets] = useState(false);

  /**
   * Fetches the list of available teams from the backend.
   * Uses the Next.js API route instead of calling gateway directly.
   */
  const loadTeams = async () => {
    setLoadingTeams(true);
    try {
      // Use relative URL to call the Next.js API route
      const response = await fetch('/api/teams');
      if (response.ok) {
        const teamsData = await response.json();
        setTeams(teamsData);
      } else {
        console.error('Failed to load teams:', response.statusText);
        toast({
          title: "Error loading teams",
          description: response.statusText,
          variant: "destructive",
        });
      }
    } catch (error) {
      console.error('Failed to load teams:', error);
      toast({
        title: "Error loading teams",
        description: (error as Error).message || "An unexpected error occurred.",
        variant: "destructive",
      });
    } finally {
      setLoadingTeams(false);
    }
  };

  /**
   * Fetches the list of available datasets from the backend.
   * Uses the Next.js API route instead of calling gateway directly.
   */
  const loadDatasets = async () => {
    setLoadingDatasets(true);
    try {
      // Use relative URL to call the Next.js API route
      const response = await fetch('/api/datasets');
      if (response.ok) {
        const data = await response.json();
        // Assuming the backend response structure has a 'datasets' key
        setDatasets(data.datasets || []);
      } else {
        console.error('Failed to load datasets:', response.statusText);
        setDatasets([]);
        toast({
          title: "Error loading datasets",
          description: response.statusText,
          variant: "destructive",
        });
      }
    } catch (error) {
      console.error('Failed to load datasets:', error);
      setDatasets([]);
      toast({
        title: "Error loading datasets",
        description: (error as Error).message || "An unexpected error occurred.",
        variant: "destructive",
      });
    } finally {
      setLoadingDatasets(false);
    }
  };

  // NEW: Load teams and datasets when the component mounts.
  // This ensures the data is fresh every time the page is visited.
  useEffect(() => {
    loadTeams();
    loadDatasets();
  }, []); // Empty dependency array ensures this runs only once on mount

  // Load teams and datasets specifically when switching to the registration tab.
  // This is kept to ensure data is loaded/refreshed if the user switches back and forth.
  useEffect(() => {
    if (!isLoginTab) { // If it's the register tab
      loadTeams();
      loadDatasets();
    }
  }, [isLoginTab]); 

  /**
   * Handles the form submission for both login and registration.
   */
  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault();
    clearError();

    if (isLoginTab) {
      const success = await login(username, password);
      if (!success && error) {
        // Error already handled by toast in auth-context.
      } else if (success) {
        // Success handled by toast and redirection in auth-context.
      }
    } else {
      let teamName = '';
      let datasetsToRegister: string[] | undefined = undefined;

      if (teamMode === 'existing') {
        teamName = selectedTeam;
      } else { // teamMode === 'new'
        teamName = newTeamName.trim();
        // Only pass selected datasets if creating a new team
        datasetsToRegister = selectedDatasets;
      }

      if (!teamName) {
        toast({
          title: "Registration Error",
          description: "Please select or create a team.",
          variant: "destructive",
        });
        return;
      }

      const success = await register(username, password, email, teamName, datasetsToRegister);
      if (!success && error) {
        // Error already handled by toast in auth-context.
      } else if (success) {
        toast({
          title: "Registration Successful",
          description: "Please log in with your new credentials.",
        });
        // Optionally, switch to login tab automatically after successful registration
        handleTabChange('login');
      }
    }
  };

  /**
   * Handles the change between login and register tabs.
   * Clears form fields and errors.
   */
  const handleTabChange = (value: string) => {
    setIsLoginTab(value === 'login');
    // Clear form fields when switching tabs
    setUsername('');
    setPassword('');
    setEmail('');
    setSelectedTeam('');
    setNewTeamName('');
    setSelectedDatasets([]);
    clearError();
  };

  /**
   * Toggles the selection state of a dataset.
   */
  const handleDatasetToggle = (datasetName: string) => {
    setSelectedDatasets(prev =>
      prev.includes(datasetName)
        ? prev.filter(d => d !== datasetName)
        : [...prev, datasetName]
    );
  };

  return (
    <div className="flex justify-center items-center min-h-screen bg-gray-100 dark:bg-gray-900 p-4">
      <Tabs defaultValue="login" className="w-full max-w-md" onValueChange={handleTabChange}>
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="login">Login</TabsTrigger>
          <TabsTrigger value="register">Register</TabsTrigger>
        </TabsList>

        <TabsContent value="login">
          <Card className="rounded-lg shadow-lg">
            <CardHeader>
              <CardTitle className="text-2xl">Login</CardTitle>
              <CardDescription>Access your team's entity resolution workspace.</CardDescription>
            </CardHeader>
            <CardContent>
              <form onSubmit={handleSubmit} className="space-y-4">
                <div>
                  <Label htmlFor="login-username">Username</Label>
                  <Input
                    id="login-username"
                    type="text"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    required
                    className="mt-1"
                    placeholder="e.g., user1"
                  />
                </div>
                <div>
                  <Label htmlFor="login-password">Password</Label>
                  <Input
                    id="login-password"
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    required
                    className="mt-1"
                    placeholder="Your password"
                  />
                </div>
                <Button type="submit" disabled={isLoading} className="w-full">
                  {isLoading ? 'Processing...' : 'Login'}
                </Button>
              </form>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="register">
          <Card className="rounded-lg shadow-lg">
            <CardHeader>
              <CardTitle className="text-2xl">Register</CardTitle>
              <CardDescription>Create a new profile and join or create a team.</CardDescription>
            </CardHeader>
            <CardContent>
              <form onSubmit={handleSubmit} className="space-y-4">
                <div>
                  <Label htmlFor="register-username">Username</Label>
                  <Input
                    id="register-username"
                    type="text"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    required
                    className="mt-1"
                    placeholder="Choose a username"
                  />
                </div>

                <div>
                  <Label htmlFor="register-email">Email</Label>
                  <Input
                    id="register-email"
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    required
                    className="mt-1"
                    placeholder="your.email@example.com"
                  />
                  <p className="mt-1 text-xs text-gray-500 dark:text-gray-400">Only whitelisted emails can register.</p>
                </div>

                <div>
                  <Label htmlFor="register-password">Password</Label>
                  <Input
                    id="register-password"
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    required
                    minLength={6}
                    className="mt-1"
                    placeholder="Create a password (min. 6 characters)"
                  />
                </div>

                {/* Team Selection */}
                <div className="space-y-4 border-t pt-4">
                  <Label className="text-base font-semibold">Team Selection</Label>

                  <Tabs value={teamMode} onValueChange={(value) => setTeamMode(value as 'existing' | 'new')}>
                    <TabsList className="grid w-full grid-cols-2">
                      <TabsTrigger value="existing">Join Existing Team</TabsTrigger>
                      <TabsTrigger value="new">Create New Team</TabsTrigger>
                    </TabsList>

                    <TabsContent value="existing" className="space-y-3">
                      <div>
                        <Label htmlFor="team-select">Select Team</Label>
                        <Select value={selectedTeam} onValueChange={setSelectedTeam}>
                          <SelectTrigger className="mt-1">
                            <SelectValue placeholder={loadingTeams ? "Loading teams..." : "Choose a team"} />
                          </SelectTrigger>
                          <SelectContent>
                            {teams.map((team) => (
                              <SelectItem key={team.id} value={team.name}>
                                {team.displayName || team.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        {loadingTeams && <p className="text-xs text-gray-500 mt-1">Loading available teams...</p>}
                      </div>
                    </TabsContent>

                    <TabsContent value="new" className="space-y-3">
                      <div>
                        <Label htmlFor="new-team-name">New Team Name</Label>
                        <Input
                          id="new-team-name"
                          type="text"
                          value={newTeamName}
                          onChange={(e) => setNewTeamName(e.target.value)}
                          className="mt-1"
                          placeholder="e.g., WA211, IL211, etc."
                        />
                      </div>

                      <div>
                        <Label className="text-sm font-medium">Select Datasets for Team</Label>
                        <p className="text-xs text-gray-500 mb-2">Choose which data sources your team will have access to:</p>
                        <div className="space-y-2 max-h-32 overflow-y-auto border rounded p-2">
                          {loadingDatasets ? (
                            <p className="text-xs text-gray-500">Loading available datasets...</p>
                          ) : datasets.length === 0 ? (
                            <p className="text-xs text-gray-500">No datasets available.</p>
                          ) : (
                            datasets.map((dataset) => (
                              <div key={dataset.name} className="flex items-start space-x-2">
                                <Checkbox
                                  id={`dataset-${dataset.name}`}
                                  checked={selectedDatasets.includes(dataset.name)}
                                  onCheckedChange={() => handleDatasetToggle(dataset.name)}
                                />
                                <div className="flex-1 min-w-0">
                                  <Label
                                    htmlFor={`dataset-${dataset.name}`}
                                    className="text-sm font-normal cursor-pointer"
                                  >
                                    {dataset.displayName}
                                  </Label>
                                  {dataset.description && (
                                    <p className="text-xs text-gray-500">{dataset.description}</p>
                                  )}
                                </div>
                              </div>
                            ))
                          )}
                        </div>
                      </div>
                    </TabsContent>
                  </Tabs>
                </div>

                <Button type="submit" disabled={isLoading} className="w-full">
                  {isLoading ? 'Processing...' : 'Register'}
                </Button>
              </form>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
