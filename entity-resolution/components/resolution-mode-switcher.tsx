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
    <div className="flex space-x-2 mb-4">
      <Button
        variant={resolutionMode === "entity" ? "default" : "outline"}
        onClick={() => handleModeChange("entity")}
        className="flex items-center"
      >
        <Network className="h-4 w-4 mr-2" />
        Entity Resolution
      </Button>
      <Button
        variant={resolutionMode === "service" ? "default" : "outline"}
        onClick={() => handleModeChange("service")}
        className="flex items-center"
      >
        <ServerCog className="h-4 w-4 mr-2" />
        Service Resolution
      </Button>
    </div>
  );
}
