// components/resolution-mode-switcher.tsx
"use client";

import { useEntityResolution } from "@/context/entity-resolution-context";
import { Button } from "@/components/ui/button";
import { Network, ServerCog } from "lucide-react"; // Using ServerCog for services

export type ResolutionMode = "entity" | "service";

export default function ResolutionModeSwitcher() {
  const { resolutionMode, actions } = useEntityResolution();

  const handleModeChange = (mode: ResolutionMode) => {
    actions.setResolutionMode(mode);
  };

  return (
    <div className="flex flex-col sm:flex-row gap-2 w-full sm:w-auto">
      <Button
        variant={resolutionMode === "entity" ? "default" : "outline"}
        onClick={() => handleModeChange("entity")}
        className="flex items-center justify-center sm:justify-start w-full sm:w-auto"
        size="sm"
      >
        <Network className="h-3.5 w-3.5 sm:h-4 sm:w-4 mr-1.5 sm:mr-2 flex-shrink-0" />
        <span className="text-xs sm:text-sm truncate">Entity Resolution</span>
      </Button>
      <Button
        variant={resolutionMode === "service" ? "default" : "outline"}
        onClick={() => handleModeChange("service")}
        className="flex items-center justify-center sm:justify-start w-full sm:w-auto"
        size="sm"
      >
        <ServerCog className="h-3.5 w-3.5 sm:h-4 sm:w-4 mr-1.5 sm:mr-2 flex-shrink-0" />
        <span className="text-xs sm:text-sm truncate">Service Resolution</span>
      </Button>
    </div>
  );
}
