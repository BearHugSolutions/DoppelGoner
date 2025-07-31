// components/resolution-mode-switcher.tsx
"use client";

import { useEntityResolution } from "@/context/entity-resolution-context";
import { Button } from "@/components/ui/button";
import { Building, Users2 } from "lucide-react";

export type ResolutionMode = "entity" | "service";

export default function ResolutionModeSwitcher() {
  const { resolutionMode, actions } = useEntityResolution();

  const handleModeChange = (mode: ResolutionMode) => {
    actions.setResolutionMode(mode);
  };

  return (
    <div className="flex flex-col sm:flex-row gap-2 w-full">
        <Button
          variant={resolutionMode === "entity" ? "default" : "outline"}
          onClick={() => handleModeChange("entity")}
          className="flex-1 justify-center text-xs h-auto py-2 px-2"
          size="sm"
        >
          <Building className="h-3 w-3 mr-0.5 flex-shrink-0" />
          <span className="text-xs sm:text-sm truncate">
            Organization
          </span>
        </Button>
        <Button
          variant={resolutionMode === "service" ? "default" : "outline"}
          onClick={() => handleModeChange("service")}
          className="flex-1 justify-center text-xs h-auto py-2 px-2"
          size="sm"
        >
          <Users2 className="h-3 w-3 mr-0.5 flex-shrink-0" />
          <span className="text-xs sm:text-sm truncate">
            Service
          </span>
        </Button>
    </div>
  );
}
